from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pytest
from sqlalchemy import select

from app import article_enrichment as coverage
from app.models import Article, ArticleEnrichment
from app.reader_archive import build_publisher_document, save_publisher_archive
from tests.test_article_enrichment import case

FIXTURES = Path(__file__).parent/'fixtures/publisher_reader'
CASES = [
    ('john-hancock.html','prnewswire','https://www.prnewswire.com/news-releases/john-hancock-closed-end-funds-declare-monthly-distributions-302926958.html','John Hancock Closed-End Funds Declare Monthly Distributions','2026-11-02T21:15:00Z','prn:302926958'),
    ('invesco.html','prnewswire','https://www.prnewswire.com/news-releases/invesco-closed-end-funds-declare-dividends-302926053.html','Invesco Closed-End Funds Declare Dividends','2026-11-02T17:00:00Z','prn:302926053'),
    ('guggenheim.html','globenewswire','https://www.globenewswire.com/news-release/2026/11/02/3393802/16530/en/guggenheim-investments-announces-november-2026-closed-end-fund-distributions.html','Guggenheim Investments Announces November 2026 Closed-End Fund Distributions','2026-11-02T21:45:00Z','gnw:3393802:en'),
]


def metadata(row):
    _,source,url,title,published,_=row
    return dict(source_code=source,page_url=url,article_url=url,title=title,published_at=published)


@pytest.mark.parametrize('row',CASES)
def test_real_saved_publisher_documents_keep_tables_and_identity(row, scratch_dir):
    content=(FIXTURES/row[0]).read_text(encoding='utf-8')
    doc=build_publisher_document(content,**metadata(row))
    assert doc['wire_key']==row[-1]
    assert doc['identity_validation']=='canonical_title_date'
    assert doc['body_kind']=='publisher_body'
    assert any(b['type']=='table' for b in doc['blocks'])
    assert any(b['type']=='paragraph' for b in doc['blocks'])
    assert save_publisher_archive(scratch_dir,42,content,**metadata(row))=='archived'
    first=(scratch_dir/'42.json').read_bytes()
    assert save_publisher_archive(scratch_dir,42,content,**metadata(row))=='already_archived'
    assert (scratch_dir/'42.json').read_bytes()==first


@pytest.mark.parametrize('override',[
    {'title':'A different distribution announcement'},
    {'published_at':'2026-11-03T21:15:00Z'},
    {'published_at':'2026-11-02'},
    {'page_url':'https://www.prnewswire.com/news-releases/different-302926959.html'},
    {'article_url':'https://www.prnewswire.com/news-releases/different-302926959.html'},
    {'page_url':'https://www.prnewswire.com.evil.test/news-releases/x-302926958.html'},
    {'page_url':'https://user:secret@www.prnewswire.com/news-releases/x-302926958.html'},
])
def test_mismatched_release_metadata_never_archived(override,scratch_dir):
    row=CASES[0]
    with pytest.raises(ValueError):
        save_publisher_archive(scratch_dir,42,(FIXTURES/row[0]).read_text(encoding='utf-8'),**{**metadata(row),**override})
    assert not (scratch_dir/'42.json').exists()


def test_ambiguous_body_and_imprecise_publisher_time_are_not_full_bodies():
    row=CASES[0];content=(FIXTURES/row[0]).read_text(encoding='utf-8')
    with pytest.raises(ValueError,match='date'):
        build_publisher_document(content.replace('2026-11-02T21:15:00Z','2026-11-02'),**metadata(row))
    with pytest.raises(ValueError,match='ambiguous'):
        build_publisher_document(content.replace('</body>','<div class="release-body"><div class="release-content">unrelated</div></div></body>'),**metadata(row))


@pytest.mark.parametrize('charset', ['', '<meta charset="windows-1252">'])
def test_already_decoded_publisher_text_preserves_unicode(charset, scratch_dir):
    _, source, url, _, published, _ = CASES[0]
    title = 'Fonds d’été'
    paragraph = 'Distribution de 12 € – annoncée à Montréal.'
    content = (f'<html><head>{charset}<link rel="canonical" href="{url}">'
               f'<meta property="og:title" content="{title}">'
               f'<meta name="date" content="{published}"></head><body><h1>{title}</h1>'
               f'<div class="release-body"><div class="release-content"><p>{paragraph}</p>'
               '</div></div></body></html>')
    assert save_publisher_archive(scratch_dir, 42, content, source_code=source, page_url=url,
                                  article_url=url, title=title, published_at=published) == 'archived'
    doc = json.loads((scratch_dir / '42.json').read_text(encoding='utf-8'))
    assert doc['title'] == title
    assert doc['blocks'] == [{'type': 'paragraph', 'text': paragraph}]
    assert doc['source_html_sha256'] == hashlib.sha256(content.encode('utf-8')).hexdigest()


def test_existing_conflicting_archive_is_preserved(scratch_dir):
    row=CASES[0];(scratch_dir/'42.json').write_text(json.dumps({'article_id':42,'wire_key':'prn:99'}))
    original=(scratch_dir/'42.json').read_bytes()
    with pytest.raises(ValueError,match='conflict'):
        save_publisher_archive(scratch_dir,42,(FIXTURES/row[0]).read_text(encoding='utf-8'),**metadata(row))
    assert (scratch_dir/'42.json').read_bytes()==original
    assert not list(scratch_dir.glob('.publisher-*'))


def test_archive_disk_failure_cannot_replay_finished_job(case,monkeypatch,scratch_dir):
    assert case.ingest()['items_inserted']==1
    calls=[]
    def failure(*args,**kwargs):
        # Separate connection sees the completed mapping transaction.
        with case.factory() as db:
            assert db.scalar(select(ArticleEnrichment.status))=='notify'
        calls.append(kwargs)
        raise OSError('simulated full disk')
    monkeypatch.setattr('app.reader_archive.save_publisher_archive',failure)
    assert coverage.process_one(case.factory,archive_directory=scratch_dir)['added_tickers']==1
    assert len(calls)==1
    assert coverage.process_one(case.factory,archive_directory=scratch_dir) is None
    with case.factory() as db:
        assert db.scalar(select(ArticleEnrichment.status))=='done'
    assert case.notifications==[1,0]


def test_successful_enrichment_archives_without_extra_fetch(case,monkeypatch,scratch_dir):
    case.ingest()
    row=CASES[0]
    with case.factory() as db:
        article=db.scalar(select(Article));article.title=row[3];article.canonical_url=row[2]
        article.published_at=datetime.fromisoformat(row[4].replace('Z','+00:00'))
        job=db.scalar(select(ArticleEnrichment));job.page_url=row[2]
        db.commit()
    requests=[]
    def fetch(*args):
        requests.append(args)
        return (FIXTURES/row[0]).read_text(encoding='utf-8')
    monkeypatch.setattr(coverage,'_fetch_source_page_html',fetch)
    coverage.process_one(case.factory,archive_directory=scratch_dir)
    assert len(requests)==1
    files=list(scratch_dir.glob('*.json'))
    assert len(files)==1 and json.loads(files[0].read_text())['wire_key']==row[-1]
    assert coverage.process_one(case.factory,archive_directory=scratch_dir) is None


def test_legacy_http_wire_reference_can_use_matching_https_canonical():
    row=CASES[0]
    doc=build_publisher_document((FIXTURES/row[0]).read_text(encoding='utf8'),
        **{**metadata(row),'article_url':row[2].replace('https:','http:')})
    assert doc['original_url'].startswith('https:') and doc['wire_key']==row[-1]


def inline_case(case,monkeypatch):
    from app import article_ingest, ticker_extraction
    from tests.test_article_enrichment import HTML
    entry=case.entries[0];entry['summary']=''
    content=(f'<html><head><link rel="canonical" href="{entry["link"]}">'
             f'<meta name="date" content="{entry["published"]}"></head><body>'
             +'<div class="release-body"><div class="release-content">'+HTML+'</div></div></body></html>')
    calls=[]
    def fetch(*args):
        calls.append(args)
        return content
    monkeypatch.setattr(article_ingest,'_extract_source_fallback_tickers',ticker_extraction._extract_source_fallback_tickers)
    monkeypatch.setattr(ticker_extraction,'_fetch_source_page_html',fetch)
    return calls


@pytest.mark.parametrize('fail_disk',[False,True])
def test_inline_body_archives_after_commit_and_notification_without_replay(case,monkeypatch,scratch_dir,fail_disk):
    from app import reader_archive
    calls=inline_case(case,monkeypatch)
    destination=scratch_dir/'archives';saved=reader_archive.save_publisher_archive
    def archive(*args,**kwargs):
        with case.factory() as db:
            assert db.scalar(select(Article.id))==kwargs['article_id']
        assert case.notifications==[1]  # Headline notification precedes formatting.
        if fail_disk: raise OSError('simulated full disk')
        return saved(*args,**kwargs)
    monkeypatch.setattr(reader_archive,'save_publisher_archive',archive)
    assert case.ingest(reader_archive_directory=str(destination))['items_inserted']==1
    assert len(calls)==1
    assert len(list(destination.glob('*.json')))==(0 if fail_disk else 1)
    assert case.ingest(reader_archive_directory=str(destination))['items_inserted']==0
    assert len(calls)==1 and case.notifications==[1]


def test_inline_transaction_failure_does_not_leave_reader_archive(case,monkeypatch,scratch_dir):
    from app import article_ingest
    from sqlalchemy.exc import SQLAlchemyError
    inline_case(case,monkeypatch)
    def fail(*args,**kwargs): raise SQLAlchemyError('simulated persistence failure')
    monkeypatch.setattr(article_ingest,'_persist_raw_feed_item',fail)
    destination=scratch_dir/'archives'
    assert case.ingest(reader_archive_directory=str(destination))['items_inserted']==0
    assert not list(destination.glob('*.json')) and case.notifications==[]


def test_body_capture_is_bounded_scoped_and_thread_isolated():
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from app.page_body_capture import capture_page_body,retain_page_body,captured_page_body,MAX_BYTES
    barrier=Barrier(2)
    def run(value):
        with capture_page_body():
            retain_page_body(value,'prnewswire',value)
            barrier.wait(timeout=3)
            return captured_page_body()['html_text']
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert list(pool.map(run,['first','second']))==['first','second']
    assert captured_page_body() is None
    with capture_page_body():
        retain_page_body('a','prnewswire','small')
        retain_page_body('b','prnewswire','x'*(MAX_BYTES+1))
        assert captured_page_body() is None
    with pytest.raises(RuntimeError),capture_page_body():
        retain_page_body('a','prnewswire','small')
        raise RuntimeError('interrupted entry')
    assert captured_page_body() is None
