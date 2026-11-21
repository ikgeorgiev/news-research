import csv
import json
from pathlib import Path
import pytest
from app import ticker_extraction as te
from app.constants import MIN_PERSIST_CONFIDENCE, CONFIDENCE_EXCHANGE
from app.utils import html_to_plain_text
FIXTURES = Path(__file__).parent / 'fixtures'
with (FIXTURES / 'correctness/registry-at-start.csv').open(encoding='utf-8-sig', newline='') as registry:
    REG = [row for row in csv.DictReader(registry) if row['active'].upper() == 'TRUE']
CALAMOS={'CHI','CHY','CSQ','CGO','CHW','CCD','CPZ'}
PROFILES=te._build_symbol_keywords([(i,r['ticker'],r['fund_name'],r['sponsor'],r['validation_keywords']) for i,r in enumerate(REG)])
KNOWN={r['ticker'] for r in REG}

def hits(title, summary='', link='https://example.com/release', known=None):
    return te._extract_entry_tickers(title,summary,link,'',KNOWN if known is None else known,symbol_keywords=PROFILES)

def verified(h):
    return {s for s,(_,confidence) in h.items() if confidence>=MIN_PERSIST_CONFIDENCE}

def test_actual_primary_calamos_headline():
    entry = json.loads((FIXTURES / 'ticker_validation/calamos.json').read_text(encoding='utf-8'))
    actual = hits(entry['title'], html_to_plain_text(entry['summary']), entry['link'])
    assert verified(actual)==CALAMOS
    assert all(actual[s]==('exchange',CONFIDENCE_EXCHANGE) for s in CALAMOS)

@pytest.mark.parametrize('value',[
    'CHI, CHY, CSQ, CGO, CHW, CCD and CPZ',
    'CHI, CHY, CSQ, CGO, CHW, CCD, and CPZ',
    'CHI,CHY,CSQ,CGO,CHW,CCD,CPZ',
    'CHI & CPZ','CHI and CPZ','CHI AND CPZ','CHI,\tCPZ',
    'CHI, CHY & CPZ','CHI,\u00a0CPZ'
])
def test_strict_complete_lists(value):
    expected=set(value.replace('and',',').replace('AND',',').replace('&',',').replace(',',' ').split())
    assert set(te._explicit_exchange_group_symbols(f'Fund news (NASDAQ: {value})'))==expected

@pytest.mark.parametrize('value',[
    'CHI, CPZ announces news','CHI CPZ','CHI or CPZ','CHI; CPZ','CHI / CPZ',
    'CHI, cpz','CHI, CPZ.P','CHI, CPZ-PR','CHI, CPZ1','CHI, 2026',
    'CHI, (CPZ)','CHI, CPZ\n','CHI, CPZ and','CHI, CPZ,','CHI,,CPZ',
    'CHI, CPZ: GOF','CHI,\nCPZ','CHI, CPZ &','CHI, and CPZ and GOF',
    'CHI, AND, CPZ','CHI, OR, CPZ','AND, CHI, CPZ'
])
def test_malformed_list_never_promotes(value):
    assert te._explicit_exchange_group_symbols(f'Funds (NASDAQ: {value})')==[]

@pytest.mark.parametrize('text',[
    'NASDAQ: CHI, CPZ','(NASD: CHI, CPZ)','(NASDAQ CHI, CPZ)',
    '(NASDAQ: CHI, CPZ','NASDAQ: CHI, CPZ)','(CHI, CPZ)',
    '(NASDAQ: CHI) and CPZ','(NASDAQ: CHI), CPZ','(NASDAQ: CHI), (CPZ)',
    '(NASDAQ:CHI,CHI)'
])
def test_missing_list_envelope_or_distinct_symbols(text):
    assert te._explicit_exchange_group_symbols(text)==[]

@pytest.mark.parametrize('exchange',['NYSE','NYSE American','NYSE Arca','NYSE MKT','NASDAQ','AMEX','OTC','OTCQB','OTCQX'])
def test_existing_exchange_vocabulary(exchange):
    assert te._explicit_exchange_group_symbols(f'({exchange}: CHI, CPZ)')==['CHI','CPZ']

def test_symbols_outside_group_not_promoted():
    result=hits('Fund notice (NASDAQ: CHI, CPZ) then CCD')
    assert verified(result)=={'CHI','CPZ'}

def test_unknown_symbol_not_assigned():
    assert verified(hits('Funds (NASDAQ: CHI, ZZZZZ and CPZ)'))=={'CHI','CPZ'}

def test_disabled_symbol_not_assigned():
    assert verified(hits('Funds (NASDAQ: CHI, CPZ)',known=KNOWN-{'CPZ'}))=={'CHI'}

def test_disabled_first_symbol_does_not_disable_valid_second_symbol():
    assert verified(hits('Funds (NASDAQ: CHI, CPZ)',known=KNOWN-{'CHI'}))=={'CPZ'}

def test_summary_group_eligible():
    assert verified(hits('Monthly distributions','Funds (NASDAQ: CHI, CPZ)'))=={'CHI','CPZ'}

def test_body_only_group_keeps_prior_policy():
    result=te._collect_segment_hits([('Funds (NASDAQ: CHI, CPZ)',te.EVIDENCE_CHANNEL_BODY_TOKEN)],KNOWN,symbol_keywords=PROFILES)
    assert verified(result)=={'CHI'}

def test_slug_group_keeps_prior_policy():
    result=te._collect_segment_hits([('https://example.com/(NASDAQ: CHI, CPZ)',te.EVIDENCE_CHANNEL_SLUG_TOKEN)],KNOWN,symbol_keywords=PROFILES)
    assert verified(result)=={'CHI'}

def test_long_group_fails_closed():
    assert te._explicit_exchange_group_symbols('(NASDAQ: '+','.join(['CHI','CPZ']*100)+')')==[]

def test_two_groups_independent():
    assert verified(hits('Funds (NASDAQ: CHI, CPZ) and (NYSE: USA, ASG)'))=={'CHI','CPZ','USA','ASG'}
