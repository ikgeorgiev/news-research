from copy import deepcopy
import json
import pytest

from app.source_research.observations import compare, identity, read_document, MAX_BYTES

URL='https://www.businesswire.com/news/home/20261102000000/en/Example'
TITLE='Example Closed-End Fund Distribution'


def observation(route='fc',seconds=20,**updates):
    value=dict(route=route,original_url=URL,title=TITLE,
        identity_evidence=dict(original_url=URL,title=TITLE,reference='synthetic-fixture-not-a-live-sample'),
        classification='new',receipt_kind='network_response',clock_id='one-machine',
        received_at=f'2026-11-02T14:00:{seconds:02d}Z',publication_label='2026-11-02T14:00:00Z',publication_precision='second')
    value.update(updates)
    return value


def result(*rows):
    return compare(dict(version=1,observations=list(rows)))


def test_receipt_comparison_and_label_proxy_stay_separate():
    r=result(observation(),observation('bw',received_at='2026-11-02T14:03:00Z'))
    assert r['matched_receipt_comparisons'][0]['b_minus_a_receipt_seconds']==-160
    assert r['routes']['fc']['within_60s']==1
    assert r['routes']['bw']['fraction_within_60s']==0
    assert len(r['routes']['bw']['label_proxy_outliers'])==1


@pytest.mark.parametrize('updates,reason',[
    ({'classification':'baseline'},'baseline_or_history_not_speed_sample'),
    ({'classification':'unknown_history'},'baseline_or_history_not_speed_sample'),
    ({'receipt_kind':'local_cache'},'not_an_independent_network_receipt'),
    ({'receipt_kind':'publisher_time'},'not_an_independent_network_receipt'),
    ({'received_at':None},'missing_receipt_time_or_clock'),
    ({'received_at':'2026-11-02T14:00:20'},'missing_receipt_time_or_clock'),
    ({'identity_evidence':None},'missing_matching_reviewed_evidence'),
    ({'original_url':'https://wallstreet-online.de/nachricht/1234'},'missing_original_wire_or_title'),
])
def test_unverified_or_nonindependent_rows_are_not_speed_samples(updates,reason):
    r=result(observation(**updates))
    assert r['accepted_first_receipts']==0 and r['excluded'][0]['reason']==reason


def test_duplicate_keeps_independent_first_sighting():
    r=result(observation(seconds=40),observation(seconds=20),observation(seconds=50))
    assert r['accepted_first_receipts']==1 and r['duplicate_input_rows']==[0,2]
    assert r['routes']['fc']['entries'][0]['publication_label_to_receipt_s']==20


@pytest.mark.parametrize('precision,value',[('minute','2026-11-02T14:00:00Z'),('date','2026-11-02')])
def test_imprecise_labels_are_not_subminute_samples(precision,value):
    r=result(observation(publication_precision=precision,publication_label=value))
    assert r['routes']['fc']['second_precision_label_proxy_samples']==0
    assert r['routes']['fc']['fraction_within_60s'] is None


def test_unreconciled_clocks_are_not_ranked():
    r=result(observation(),observation('godel',clock_id='vendor-clock'))
    assert not r['matched_receipt_comparisons'] and len(r['incomparable_receipt_pairs'])==1


def test_title_conflict_excludes_both_and_same_title_different_wire_is_distinct():
    other=observation('bw',title='Different')
    other['identity_evidence']['title']='Different'
    assert result(observation(),other)['accepted_first_receipts']==0
    other=observation('bw',original_url=URL.replace('000000','000001'))
    other['identity_evidence']['original_url']=other['original_url']
    assert result(observation(),other)['known_original_wires']==2


def test_locale_ids_are_distinct_and_hosts_must_be_exact():
    assert identity(URL.replace('/en/','/fr/')) != identity(URL)
    assert identity(URL.replace('www.businesswire.com','businesswire.com.evil.test')) is None
    assert identity(URL.replace('www.businesswire.com','user:pass@www.businesswire.com')) is None
    assert identity('https://www.prnewswire.com/news-releases/test-12345.html')=='prn:12345'


def test_negative_proxy_is_not_fast_success():
    r=result(observation(publication_label='2026-11-02T15:00:00Z'))
    assert r['routes']['fc']['within_60s']==0
    assert r['routes']['fc']['entries'][0]['proxy_status'].startswith('negative_')


def test_input_is_immutable_and_ticker_labels_are_not_validation():
    row=observation(related_tickers=['XYZ']); before=deepcopy(row)
    r=result(row)
    assert row==before and 'validated_tickers' not in r['routes']['fc']['entries'][0]


def test_file_and_record_bounds(tmp_path):
    p=tmp_path/'oversized.json'; p.write_bytes(b' '*(MAX_BYTES+1))
    with pytest.raises(ValueError,match='5 MiB'): read_document(p)
    with pytest.raises(ValueError,match='1000'): result(*[{}]*1001)
    p.write_text(json.dumps(dict(version=1,observations=[])))
    assert compare(read_document(p))['accepted_first_receipts']==0
