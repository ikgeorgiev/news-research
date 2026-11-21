import json
import pytest
from app.source_research.yahoo_direct import assess

NOW='2026-11-07T15:00:00+00:00'
ASSET={'__typename':'Story','id':'id-1','title':'Fund distribution',
       'contentAttributes':{'canonicalUrl':'https://finance.yahoo.com/news/example.html',
           'publishTime':'2026-11-06T14:00:00Z','displayTime':'2026-11-07T14:00:00Z'},
       'finance':{'stockTickers':[{'symbol':'OXLC'}]}}


def payload(assets, **other):
    return {'data':{'lightyearList':{'main':{'edges':[{'node':{'asset':a}} for a in assets],
            'pageInfo':{'hasNextPage':False,'endCursor':None}}}}, **other}


def read(data, status=200, **kwargs):
    return assess(status,json.dumps(data).encode(),received_at=NOW,requested_symbol='OXLC',tab='press releases',**kwargs)


@pytest.mark.parametrize('status',[301,401,403,404,429,500])
def test_failed_route_never_becomes_empty_success(status):
    result=read(payload([]),status)
    assert result['status']=='http_error' and not result['absence_eligible']


@pytest.mark.parametrize('data',[{}, {'data':None}, {'data':{'lightyearList':None}}, {'tickerStream':{'stream':[]}}])
def test_old_or_missing_schema_explicit(data):
    result=read(data)
    assert result['status']=='invalid' and result['issues']


def test_partial_assets_and_graphql_errors_remain_visible():
    result=read(payload([ASSET,{'__typename':'AssetError'}],errors=[{'message':'unavailable'}]))
    assert result['status']=='partial' and len(result['items'])==1
    assert not result['absence_eligible']


def test_display_time_is_not_publication_and_ticker_is_not_assignment():
    item=read(payload([ASSET]))['items'][0]
    assert item['published_at']=='2026-11-06T14:00:00+00:00'
    assert item['display_at']=='2026-11-07T14:00:00+00:00'
    assert item['related_tickers']==[{'symbol':'OXLC'}] and item['validated_fund_tickers']==[]
    assert item['original_wire_identity'] is None and not item['speed_comparable']


def test_cache_and_valid_empty_are_not_coverage_evidence():
    assert read(payload([]))['status']=='observed'
    assert not read(payload([]))['absence_eligible']
    assert read(payload([ASSET]),network_response=False)['status']=='cached'


def test_outlinks_do_not_borrow_display_as_publication():
    item=read(payload([{'__typename':'Outlink','id':'o','title':'Fund','url':'https://www.businesswire.com/news/home/123/en/','displayTime':NOW}]))['items'][0]
    assert item['published_at'] is None and item['display_at']==NOW


def test_html_size_timezone_and_duplicate_guards():
    result=assess(200,b'<html>blocked</html>',received_at=NOW,requested_symbol='OXLC',tab='all')
    assert result['status']=='invalid'
    assert read(payload([ASSET,ASSET]))['issues']==['duplicate_asset_id']
    with pytest.raises(ValueError):
        assess(200,b'{}',received_at='2026-11-07T15:00:00',requested_symbol='OXLC',tab='all')
    assert assess(200,b'x'*(1024*1024+1),received_at=NOW,requested_symbol='OXLC',tab='all')['issues']==['response_too_large']
