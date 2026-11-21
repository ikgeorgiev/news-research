from pathlib import Path
import pytest
from sqlalchemy import create_engine,func,select
from sqlalchemy.orm import sessionmaker
from app.database import Base
from app.models import Article,ArticleTicker,RawFeedItem,Source
from tests.helpers import seed_ticker
from app.financialcontent.delivery import deliver

from app.financialcontent.financialcontent_adapter import parse_article

FIXTURES = Path(__file__).parent / 'fixtures/financialcontent'
RELEASES = {
    'validation-1902-saba': ('saba.html', 'saba-announces-0155-special-dividend',
                             'SABA Announces $0.155 Special Dividend', 'bw:20261027955822:en'),
    'validation-2059-cet': ('cet.html', 'central-securities-corporation-releases-report-to-stockholders',
                            'Central Securities Corporation Releases Report to Stockholders', 'bw:20261027473222:en'),
}

def saved_entry(folder):
    filename, slug, title, wire_key = RELEASES[folder]
    return parse_article((FIXTURES / filename).read_bytes(),
                         article_url=f'https://markets.chroniclejournal.com/thepilotnews/article/bizwire-2026-10-27-{slug}',
                         expected_title=title, expected_wire_key=wire_key)


@pytest.fixture
def factory(tmp_path):
    engine=create_engine('sqlite:///'+str(tmp_path/'app.sqlite'))
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine,autoflush=False)
    engine.dispose()


@pytest.mark.parametrize('symbol,folder',[('SABA','validation-1902-saba'),('CET','validation-2059-cet')])
def test_saved_body_uses_current_ticker_validation_and_idempotent_core(factory,symbol,folder):
    with factory() as db:seed_ticker(db,symbol=symbol)
    entry=saved_entry(folder)
    first=deliver(factory,entry,listing_seen_at='saved fixture',body_received_at='saved fixture')
    replay=deliver(factory,entry,listing_seen_at='saved fixture',body_received_at='saved fixture')
    assert first['created'] and not replay['created']
    assert first['article_id']==replay['article_id']
    with factory() as db:
        assert db.scalar(select(func.count()).select_from(Article))==1
        assert db.scalar(select(func.count()).select_from(ArticleTicker))==1
        assert db.scalar(select(func.count()).select_from(RawFeedItem))==1


def test_unvalidated_body_stays_raw_and_does_not_publish(factory):
    with factory() as db:seed_ticker(db,symbol='UNRELATED')
    result=deliver(factory,saved_entry('validation-1902-saba'),listing_seen_at='fixture',body_received_at='fixture')
    assert result['article_id'] is None and not result['created']
    with factory() as db:assert db.scalar(select(func.count()).select_from(Article))==0


def test_disabled_source_is_not_reenabled(factory):
    with factory() as db:
        db.add(Source(code='financialcontent',name='FinancialContent',base_url='https://markets.chroniclejournal.com',enabled=False));db.commit()
    with pytest.raises(RuntimeError,match='disabled'):
        deliver(factory,saved_entry('validation-1902-saba'),listing_seen_at='fixture',body_received_at='fixture')


def test_same_headline_different_original_wire_identity_is_held(factory):
    with factory() as db:seed_ticker(db,symbol='SABA')
    entry=saved_entry('validation-1902-saba')
    deliver(factory,entry,listing_seen_at='fixture',body_received_at='fixture')
    different={**entry,'id':'bw:20261028000001:en','link':entry['link']+'-other',
               'source':{'title':'Business Wire','href':'https://www.businesswire.com/news/home/20261028000001/en/'}}
    with pytest.raises(ValueError,match='wire identity'):
        deliver(factory,different,listing_seen_at='fixture',body_received_at='fixture')
