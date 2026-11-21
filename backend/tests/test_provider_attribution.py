from datetime import datetime, timezone

import pytest
from sqlalchemy import select

from app.models import RawFeedItem, Source
from app.routes.news import get_news_item, list_news, list_news_ids
from tests.helpers import seed_article


@pytest.mark.parametrize(
    ('first_code', 'first_name', 'first_url', 'later_code', 'later_name', 'later_url'),
    [
        ('businesswire', 'Business Wire', 'https://www.businesswire.com/news/home/20261028524655/en',
         'yahoo', 'Yahoo Finance', 'https://finance.yahoo.com/news/virtus-ztr.html'),
        ('yahoo', 'Yahoo Finance', 'https://finance.yahoo.com/news/virtus-ztr.html',
         'businesswire', 'Business Wire', 'https://www.businesswire.com/news/home/20261028524655/en'),
        ('prnewswire', 'PR Newswire', 'https://www.prnewswire.com/news-releases/royce.html',
         'yahoo', 'Yahoo Finance', 'https://finance.yahoo.com/news/royce.html'),
    ],
)
def test_tracking_url_duplicate_keeps_original_badge_and_provider_filters(
    db_session, first_code, first_name, first_url, later_code, later_name, later_url,
):
    db = db_session
    original = Source(code=first_code, name=first_name, base_url=first_url, enabled=True)
    mirror = Source(code=later_code, name=later_name, base_url=later_url, enabled=True)
    db.add_all([original, mirror])
    db.commit()
    article = seed_article(
        db, slug='tracking-provider', canonical_url=first_url,
        source_name=first_name, provider_name=first_name,
        published_at=datetime(2026, 10, 28, 20, 20, tzinfo=timezone.utc),
    )
    first_seen = article.first_seen_at
    db.add(RawFeedItem(
        article_id=article.id, source_id=original.id, feed_url=first_url,
        raw_guid='first', raw_link=first_url + '?feedref=original',
        raw_pub_date=article.published_at, raw_payload_json={},
    ))
    db.commit()
    assert get_news_item(article.id, db=db).provider == first_name

    # Both source URLs contain tracking parameters removed from the article URL.
    db.add(RawFeedItem(
        article_id=article.id, source_id=mirror.id, feed_url=later_url,
        raw_guid='mirror', raw_link=later_url + '?.tsrc=rss',
        raw_pub_date=article.published_at, raw_payload_json={},
    ))
    db.commit()
    options = dict(include_unmapped=True, include_unmapped_from_provider=None,
                   from_=None, to=None, limit=10, cursor=None, db=db)
    assert get_news_item(article.id, db=db).provider == first_name
    page = list_news(provider=first_name, **options)
    assert [(item.id, item.provider) for item in page.items] == [(article.id, first_name)]
    assert list_news_ids(provider=first_name, **options).ids == [article.id]
    assert list_news(provider=later_name, **options).items == []
    assert list_news_ids(provider=later_name, **options).ids == []
    db.refresh(article)
    assert article.first_seen_at == first_seen
    assert len(list(db.scalars(select(RawFeedItem).where(RawFeedItem.article_id == article.id)))) == 2


def test_unsubstantiated_legacy_provider_still_uses_raw_provenance(db_session):
    db = db_session
    yahoo = Source(code='yahoo', name='Yahoo Finance', base_url='https://finance.yahoo.com', enabled=True)
    db.add(yahoo)
    db.commit()
    article = seed_article(
        db, slug='stale-provider', canonical_url='https://example.com/legacy',
        provider_name='Business Wire', source_name='Business Wire',
        published_at=datetime(2026, 10, 28, tzinfo=timezone.utc),
    )
    db.add(RawFeedItem(
        article_id=article.id, source_id=yahoo.id, feed_url='https://finance.yahoo.com/rss',
        raw_link='https://finance.yahoo.com/news/legacy?.tsrc=rss',
        raw_pub_date=article.published_at, raw_payload_json={},
    ))
    db.commit()
    assert get_news_item(article.id, db=db).provider == 'Yahoo Finance'
