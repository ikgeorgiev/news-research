"""Real retained Franklin collision; all operations use fixture SQLite."""
import json
from pathlib import Path
from datetime import datetime,timezone
import pytest
from sqlalchemy import select,func
from app.models import Article,ArticleTicker,RawFeedItem,Source,Ticker
from app.article_ingest import _upsert_article,_prepare_feed_entries,_process_single_entry
from app.ticker_context import load_ticker_context

DATA=json.loads((Path(__file__).parent/'fixtures/franklin_same_headline.json').read_text(encoding='utf-8'))
TITLE='Franklin Templeton Announces Availability of 19(a) Notices for Certain Closed-End Funds'
PUB=datetime(2026,10,30,12,tzinfo=timezone.utc)
YAHOO=next(r for r in DATA['raw'] if r['code']=='yahoo')
def as_time(x):return datetime.fromisoformat(x) if x else None

def seed(db,ids=(1623,1624,1626,1627),only_fc=False):
 for r in DATA['registry']:
  db.add(Ticker(id=r['id'],symbol=r['symbol'],fund_name=r['fund_name'],sponsor=r['sponsor'],validation_keywords=r['validation_keywords'],active=True))
 source_ids={r['source_id']:r['code'] for r in DATA['raw'] if r['code']!='yahoo'}
 for sid,code in source_ids.items():db.add(Source(id=sid,code=code,name=code,base_url='https://example.invalid',enabled=True))
 for r in DATA['articles']:
  if r['id'] not in ids:continue
  values={k:v for k,v in r.items() if k!='tickers'}
  for key in ['published_at','first_seen_at','first_alert_sent_at','created_at','updated_at']:values[key]=as_time(values[key])
  if r['id']==1627:
   values['summary']=next(x['raw_payload_json']['summary'] for x in DATA['raw'] if x['article_id']==1627 and x['code']=='businesswire')
  db.add(Article(**values))
 for r in DATA['raw']:
  if r['article_id'] not in ids or r['code']=='yahoo' or (only_fc and r['code']!='financialcontent'):continue
  values={k:v for k,v in r.items() if k!='code'}
  for key in ['raw_pub_date','fetched_at']:values[key]=as_time(values[key])
  db.add(RawFeedItem(**values))
 if 1623 in ids:
  for r in DATA['registry']:
   if r['symbol'] in {'PMM','PMO'}:db.add(ArticleTicker(article_id=1623,ticker_id=r['id'],match_type='validated_token',confidence=.68))
 db.commit()

def upsert(db,code,url,summary):
 return _upsert_article(db,source_code=code,canonical_url=url,title=TITLE,summary=summary,published_at=PUB,source_name=code,provider_name=code)

def test_yahoo_copy_matches_its_body_not_the_newest_identical_headline(db_session):
 db=db_session;seed(db)
 source=Source(code='yahoo',name='Yahoo Finance',base_url='https://finance.yahoo.com',enabled=True);db.add(source);db.flush()
 payload=YAHOO['raw_payload_json'];entry={**payload,'id':YAHOO['raw_guid']}
 prepared=_prepare_feed_entries([entry],PUB,source_code='yahoo')[0];context=load_ticker_context(db)
 result=_process_single_entry(db,prepared=prepared,source=source,source_name='Yahoo Finance',provider_name='Yahoo Finance',feed_url='https://feeds.finance.yahoo.com/rss',known_symbols=context.known_symbols,symbol_to_id=context.symbol_to_id,symbol_keywords=context.symbol_keywords,timeout_seconds=1,page_config=None,recorded_guids=set(),recorded_pairs=set(),postcommit_enrichment_enabled=False)
 db.flush()
 raw=db.scalar(select(RawFeedItem).where(RawFeedItem.source_id==source.id,RawFeedItem.raw_guid==entry['id']))
 assert raw.article_id==1623
 assert not result.created_article
 assert list(db.scalars(select(ArticleTicker).where(ArticleTicker.article_id==1627)))==[]
 assert db.scalar(select(func.count()).select_from(Article))==4

def test_distinct_wire_id_does_not_merge_into_single_same_title_fc_article(db_session):
 db=db_session;seed(db,ids=(1623,),only_fc=True)
 a,created,_=upsert(db,'businesswire','https://www.businesswire.com/news/home/20261030005490/en/','FORT LAUDERDALE, Fla.--(BUSINESS WIRE)--Category: Distribution Related')
 assert created and a.id!=1623

def test_same_wire_identity_wins_even_when_another_headline_is_newer(db_session):
 db=db_session;seed(db)
 a,created,_=upsert(db,'yahoo','https://www.businesswire.com/news/home/20261030159147/en/New-Slug','BOSTON--(BUSINESS WIRE)--Category: Distribution Related')
 assert a.id==1623 and not created

def test_original_wire_url_variant_still_matches_its_existing_wire_copy(db_session):
 db=db_session;seed(db)
 a,created,_=upsert(db,'businesswire','https://www.businesswire.com/news/home/20261030159147/en/New-Slug','BOSTON--(BUSINESS WIRE)--Category: Distribution Related')
 assert a.id==1623 and not created

def test_ambiguous_title_without_matching_body_stays_separate(db_session):
 db=db_session;seed(db)
 a,created,_=upsert(db,'yahoo','https://finance.yahoo.com/news/other-franklin.html','Updated notices are available.')
 assert created and a.id not in {1623,1624,1626,1627}

def test_ambiguous_equal_bodies_are_not_broken_by_article_order(db_session):
 db=db_session;seed(db)
 db.get(Article,1627).summary=db.get(Article,1623).summary;db.flush()
 a,created,_=upsert(db,'yahoo','https://finance.yahoo.com/news/ambiguous.html',YAHOO['raw_payload_json']['summary'])
 assert created and a.id not in {1623,1624,1626,1627}

def test_unambiguous_existing_article_still_deduplicates(db_session):
 db=db_session;seed(db,ids=(1623,),only_fc=True)
 a,created,_=upsert(db,'yahoo',YAHOO['raw_link'],YAHOO['raw_payload_json']['summary'])
 assert a.id==1623 and not created

def test_exact_url_update_keeps_existing_identity(db_session):
 db=db_session;seed(db)
 original=db.get(Article,1626)
 a,created,by_url=upsert(db,'businesswire',original.canonical_url,'Verified updated coverage')
 assert a.id==1626 and not created and by_url


def test_legacy_corrected_headline_matches_only_its_current_title(db_session):
    from app.utils import normalize_title, sha256_str

    db = db_session
    article, _, _ = upsert(db, "yahoo", "https://example.com/original", "Updated notice.")
    article.title = "Corrected Quarterly Distribution Notice"
    corrected_hash = sha256_str(normalize_title(article.title))
    # Earlier releases updated the stored title and cluster but left the old title index.
    article.cluster_key = corrected_hash
    db.commit()

    _, created, _ = upsert(db, "yahoo", "https://example.com/another-release", "Different release.")
    assert created
    match, created, _ = _upsert_article(db, source_code="yahoo", canonical_url="https://example.com/mirror",
                                       title=article.title, summary=None, published_at=PUB,
                                       source_name="Yahoo Finance", provider_name="Yahoo Finance")
    assert not created and match.id == article.id
    assert match.title_normalized_hash == corrected_hash


def test_content_hash_tracks_summary_kept_after_replay_and_syndication(db_session):
    from app.utils import normalize_title, sha256_str

    db = db_session
    url = "https://example.com/original"
    article, _, _ = upsert(db, "yahoo", url, "Original summary")
    original_hash = article.content_hash
    replay, _, _ = upsert(db, "yahoo", url, None)
    assert replay.summary == "Original summary" and replay.content_hash == original_hash
    summary = "A syndicated copy with a more complete summary"
    mirror, created, _ = upsert(db, "yahoo", "https://example.com/another-copy", summary)
    assert not created
    assert mirror.content_hash == sha256_str(f"{mirror.title_normalized_hash}|{normalize_title(summary)}")
@pytest.mark.parametrize(('url','guid','source','expected'),[
 ('https://www.businesswire.com/news/home/20261030159147/en/Title?feedref=x',None,'yahoo',('businesswire','20261030159147','en')),
 ('https://www.businesswire.com/news/home/20261030159147/fr/Title',None,'businesswire',('businesswire','20261030159147','fr')),
 ('https://markets.example.test/article','bw:20261030159147:en','financialcontent',('businesswire','20261030159147','en')),
 ('https://www.prnewswire.com/news-releases/release-302915869.html',None,'yahoo',('prnewswire','302915869','')),
 ('https://www.globenewswire.com/news-release/2026/10/30/1234567/0/en/Release.html',None,'yahoo',('globenewswire','1234567','en')),
 ('https://fakebusinesswire.com/news/home/20261030159147/en/',None,'yahoo',None),
 ('https://businesswire.com.attacker.test/news/home/20261030159147/en/',None,'yahoo',None),
 ('https://finance.yahoo.com/news/release.html','20261030159147en','yahoo',None),
 ('ftp://www.businesswire.com/news/home/20261030159147/en/',None,'yahoo',None),
 ('https://user:secret@www.businesswire.com/news/home/20261030159147/en/',None,'yahoo',None),
 ('https://www.businesswire.com:8443/news/home/20261030159147/en/',None,'yahoo',None),
 ('https://[invalid',None,'yahoo',None),
])
def test_wire_identity_requires_actual_provider_provenance(url,guid,source,expected):
 from app.release_matching import release_identity
 assert release_identity(url,guid,source)==expected
