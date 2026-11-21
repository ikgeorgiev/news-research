"""Saved-page discovery only; no publisher or issuer-feed network activity."""
import json
from pathlib import Path
import socket

from lxml import html
import pytest

from app.gnw_feed_discovery import FIELD, advertised_issuer_feed
from app.reader_archive import build_publisher_document, save_publisher_archive
from tests.test_publisher_reader import CASES, metadata
from tests.test_article_enrichment import case

FIXTURES=Path(__file__).parent/'fixtures/publisher_reader'
ROW=CASES[2]
URL=ROW[2]
FEED='/rssfeed/organization/tFnNZMqUlNlLAjnUc5umPA=='
TITLE=ROW[3]


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError('Discovery must not make network requests')
    monkeypatch.setattr(socket.socket,'connect',forbidden)


def model():
    return {'SocialItemData':{'Url':URL,'Title':TITLE},
            'AdditionalItems':[{'Key':'rss','Label':'Subscribe via RSS','Url':FEED}]}


def script(value):
    payload=json.dumps(value,ensure_ascii=False).replace('\\','\\\\').replace("'","\\'").replace('"','\\"')
    return '<script>'+FIELD+" = JSON.parse('"+payload+"');</script>"


def page(value=None):
    body=('<html><head><link rel="canonical" href="'+URL+'">'
          '<meta property="article:published_time" content="'+ROW[4]+'"></head>'
          '<body><h1>'+TITLE+'</h1><div itemprop="articleBody"><p>'
          'The fund announces a distribution. This is a synthetic release fixture used only '
          'to test optional discovery metadata without any request or article insertion.</p>'
          '<table><tr><td>Distribution</td><td>$0.15</td></tr></table></div>')
    return body+(script(model() if value is None else value))+'</body></html>'


def extract(content):
    return advertised_issuer_feed(html.fromstring(content),canonical_url=URL,title=TITLE)


def test_actual_saved_guggenheim_page_retains_the_published_issuer_feed():
    content=(FIXTURES/'guggenheim.html').read_text(encoding='utf-8')
    doc=build_publisher_document(content,**metadata(ROW))
    result=doc['publisher_feed_discovery']
    assert result['status']=='advertised_link_retained'
    assert result['feeds'][0]['url']=='https://www.globenewswire.com'+FEED
    assert result['feeds'][0]['advertised_value']==FEED
    assert result['feeds'][0]['source_article_url']==URL
    assert not result['feeds'][0]['automatic_request_allowed']
    assert result['source_requests']==0 and result['source_enrollment']=='review_only'
    assert result['prior_access_decisions']=='not_evaluated_or_changed'
    assert any(block['type']=='table' for block in doc['blocks'])


@pytest.mark.parametrize('url',[
    'https://www.globenewswire.com'+FEED,
    FEED,
])
def test_only_advertised_absolute_or_root_relative_url_is_retained(url):
    value=model();value['AdditionalItems'][0]['Url']=url
    result=extract(page(value))
    assert result['status']=='advertised_link_retained'
    assert result['feeds'][0]['url']=='https://www.globenewswire.com'+FEED


@pytest.mark.parametrize('url',[
    'http://www.globenewswire.com'+FEED,
    'https://globenewswire.com'+FEED,
    '//www.globenewswire.com'+FEED,
    'https://www.globenewswire.com.evil.test'+FEED,
    'https://user:secret@www.globenewswire.com'+FEED,
    'https://www.globenewswire.com:443'+FEED,
    FEED+'?cachebust=1',FEED+'#section',
    '/rssfeed/organization/../../rss/list',
    '/rssfeed/organization/%74FnNZMqUlNlLAjnUc5umPA==',
    '/api/subscribe/follow-organization',
    '/atomfeed/organization/tFnNZMqUlNlLAjnUc5umPA==',
    '/rssfeed/organization/13336',
    'javascript:alert(1)',
    ' '+ 'https://www.globenewswire.com'+FEED,
    'https://www.globenewswire.com'+FEED+'\n',
    'https://www.globenewswire.com'+FEED.replace('organization','organi\tzation'),
])
def test_unreviewed_host_path_query_or_derived_id_is_not_promoted(url):
    value=model();value['AdditionalItems'][0]['Url']=url
    result=extract(page(value))
    assert result['status']=='metadata_rejected' and not result['feeds']
    doc=build_publisher_document(page(value),**metadata(ROW))
    assert doc['blocks'] and doc['identity_validation']=='canonical_title_date'


@pytest.mark.parametrize('change',[
    {'Url':URL.replace('3393802','3393803')},
    {'Title':'A different release'},
])
def test_feed_link_must_belong_to_the_validated_release(change):
    value=model();value['SocialItemData'].update(change)
    assert extract(page(value))['status']=='metadata_rejected'


def test_multiple_metadata_models_are_rejected_without_losing_body():
    content=page().replace('</body>',script(model())+'</body>')
    doc=build_publisher_document(content,**metadata(ROW))
    assert doc['publisher_feed_discovery']['status']=='metadata_rejected'
    assert doc['blocks']


def test_duplicate_rss_actions_are_ambiguous():
    value=model();value['AdditionalItems']*=2
    assert extract(page(value))['status']=='metadata_rejected'


def test_unlabelled_action_is_not_a_documented_feed():
    value=model();value['AdditionalItems'][0]['Label']='Related content'
    assert extract(page(value))['status']=='metadata_rejected'


def test_org_id_and_atom_action_never_generate_an_rss_endpoint():
    value=model();value['ContextOrgId']=13336
    value['AdditionalItems']=[{'Key':'atom','Label':'Subscribe via ATOM','Url':FEED.replace('rssfeed','atomfeed')}]
    result=extract(page(value))
    assert result['status']=='not_advertised' and result['feeds']==[]


def test_optional_metadata_absence_preserves_body():
    content=page().replace(script(model()),'')
    doc=build_publisher_document(content,**metadata(ROW))
    assert doc['publisher_feed_discovery']['status']=='not_advertised'
    assert doc['blocks']


def test_article_body_script_is_not_source_metadata():
    content=page().replace(script(model()),'').replace('<p>',script(model())+'<p>',1)
    assert extract(content)['status']=='not_advertised'


@pytest.mark.parametrize('payload',[
    FIELD+"=JSON.parse('{}') + unexpected_function();",
    FIELD+"=JSON.parse('{\\x22not-json\\x22}');",
    FIELD+"=JSON.parse('{\\uD800}');",
    FIELD+"=JSON.parse('{not-json}');",
    FIELD+"=JSON.parse('"+'x'*(33*1024)+"');",
], ids=['expression','hex_escape','surrogate','invalid_json','oversized'])
def test_unsupported_or_oversized_metadata_is_inert(payload):
    content=page().replace(script(model()),'<script>'+payload+'</script>')
    assert extract(content)['status']=='metadata_rejected'
    assert build_publisher_document(content,**metadata(ROW))['blocks']


def test_duplicate_json_keys_are_not_silently_accepted():
    literal=json.dumps(model()).replace('"SocialItemData":','"SocialItemData": {}, "SocialItemData":',1)
    escaped=literal.replace('\\','\\\\').replace("'","\\'").replace('"','\\"')
    content=page().replace(script(model()),'<script>'+FIELD+"=JSON.parse('"+escaped+"');</script>")
    assert extract(content)['status']=='metadata_rejected'


def test_first_archive_is_immutable_when_later_page_has_new_feed_metadata(tmp_path):
    first=page().replace(script(model()),'')
    assert save_publisher_archive(tmp_path,77,first,**metadata(ROW))=='archived'
    original=(tmp_path/'77.json').read_bytes()
    assert save_publisher_archive(tmp_path,77,page(),**metadata(ROW))=='already_archived'
    assert (tmp_path/'77.json').read_bytes()==original


def test_enrichment_body_and_hashes_do_not_depend_on_optional_metadata():
    with_feed=build_publisher_document(page(),**metadata(ROW))
    without=build_publisher_document(page().replace(script(model()),''),**metadata(ROW))
    for key in ['wire_key','blocks','text_sha256','body_published_at','title','published_at']:
        assert with_feed[key]==without[key]


def test_prnewswire_body_does_not_receive_gnw_discovery_fields():
    row=CASES[0]
    doc=build_publisher_document((FIXTURES/row[0]).read_text(encoding='utf-8'),**metadata(row))
    assert 'publisher_feed_discovery' not in doc


def test_natural_gnw_enrichment_retains_link_without_another_fetch_or_alert(case,monkeypatch,scratch_dir):
    from datetime import datetime
    from sqlalchemy import select
    from app import article_enrichment as coverage
    from app.models import Article, ArticleEnrichment, ArticleTicker, Source
    case.ingest()
    with case.factory() as db:
        source=db.get(Source,case.source_id)
        source.code='globenewswire';source.name='GlobeNewswire'
        article=db.scalar(select(Article))
        article.title=TITLE;article.canonical_url=URL
        article.published_at=datetime.fromisoformat(ROW[4].replace('Z','+00:00'))
        db.scalar(select(ArticleEnrichment)).page_url=URL
        prior_tickers=list(db.scalars(select(ArticleTicker.ticker_id)))
        db.commit()
    requests=[]
    def fetched(url,*args):
        requests.append(url)
        return (FIXTURES/'guggenheim.html').read_text(encoding='utf-8')
    monkeypatch.setattr(coverage,'_fetch_source_page_html',fetched)
    prior_alerts=list(case.notifications)
    coverage.process_one(case.factory,archive_directory=scratch_dir)
    saved=list(scratch_dir.glob('*.json'))
    assert len(saved)==1
    document=json.loads(saved[0].read_text(encoding='utf-8'))
    assert document['publisher_feed_discovery']['feeds'][0]['url']=='https://www.globenewswire.com'+FEED
    assert coverage.process_one(case.factory,archive_directory=scratch_dir) is None
    assert requests==[URL] and case.notifications==prior_alerts
    with case.factory() as db:
        assert list(db.scalars(select(ArticleTicker.ticker_id)))==prior_tickers
        assert db.scalar(select(ArticleEnrichment.status))=='done'


def test_optional_discovery_is_omitted_before_it_breaks_reader_size_limit(monkeypatch):
    import app.reader_archive as reader
    normal=build_publisher_document(page(),**metadata(ROW))
    without={k:v for k,v in normal.items() if k!='publisher_feed_discovery'}
    cap=len(json.dumps(without,ensure_ascii=False).encode())+128
    monkeypatch.setattr(reader,'MAX_ARCHIVE_BYTES',cap)
    # Synthetic HTML fits this cap; the optional field would exceed it.
    assert len(page().encode())<cap
    doc=reader.build_publisher_document(page(),**metadata(ROW))
    assert 'publisher_feed_discovery' not in doc and doc['blocks']
