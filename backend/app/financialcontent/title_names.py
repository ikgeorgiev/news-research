"""Fetch-only title recall from fresh registry names and reviewed name evidence.

This module never assigns a ticker, changes the universe, or delivers an article.
The existing body/original-wire/ticker/deduplication checks remain authoritative.
"""
from functools import lru_cache
import html
import json
from pathlib import Path
import re
from types import SimpleNamespace
import unicodedata

ALIASES_PATH=Path(__file__).with_name('reviewed_name_aliases.json')
# Lexical equivalence only; no issuer takeover, renamed strategy, or fuzzy edits.
REPLACEMENTS={
    'fd':'fund','tr':'trust','tru':'trust','grd':'grade','invstm':'investment',
    'opp':'opportunities','opps':'opportunities','opportunity':'opportunities',
    'mun':'municipal','muni':'municipal','glbl':'global','glb':'global',
    'intl':'international','div':'dividend','pref':'preferred','inc':'income',
    'conv':'convertible','corp':'corporate','cred':'credit','infra':'infrastructure','dur':'duration',
    'mgmt':'management','strats':'strategies','strgy':'strategy','prem':'premium',
    'eq':'equity','secs':'securities','sec':'securities','qual':'quality','qty':'quality',
    'aberdeen':'abrdn',
    'fr':'floating rate','multimarket':'multi market',
    # Paired primary issuer examples are retained in universe-name-audit/
    # wire-conventions/paired-expansion-fixtures.json. No ambiguous Gr/Adv map.
    'ca':'california','ny':'new york','nj':'new jersey','pa':'pennsylvania',
    'va':'virginia','mi':'michigan','az':'arizona','ma':'massachusetts',
    'mn':'minnesota','mo':'missouri','jhancock':'john hancock',
    'flah':'flaherty','crum':'crumrine','tx':'tax','mgd':'managed','rt':'rate',
}
CORPORATE={'inc','llc','ltd','plc','corp','corporation','co','company','ord','common','unit'}
FINANCIAL=re.compile(r'\b(?:distributions?|dividends?|earnings|financial|stockholders?|shareholders?|tender|repurchases?|rights|nav|portfolio|capital gains|discount management|19 a|19a)\b')
GROUP_EVENT=re.compile(r'\b(?:distributions?|dividends?|tender|repurchases?|reorganization|reorganizations|merger|mergers|liquidation|earnings|financial results|discount management|19 a|19a|portfolio|webinar|annual meeting|special meeting|shareholder meetings?|agreement)\b')
SERIES={'ii','iii','iv','v','vi','vii','viii','ix','x'}

@lru_cache(maxsize=4096)
def raw_words(value):
    value=html.unescape(html.unescape(unicodedata.normalize('NFKC',value or '')))
    value=re.sub(r'\bL\s*/\s*S\b','long short',value,flags=re.IGNORECASE)
    return tuple(w for w in re.findall(r'[a-z0-9]+',value.casefold()) if w not in {'and','the'})

@lru_cache(maxsize=4096)
def words(value):
    expanded=tuple(part for w in raw_words(value) for part in REPLACEMENTS.get(w,w).split())
    return tuple(w for i,w in enumerate(expanded) if not (w in {'fund','trust'} and i+1<len(expanded) and expanded[i+1] in SERIES))

@lru_cache(maxsize=2048)
def variants(value):
    raw=list(raw_words(value));result=[]
    if raw:result.append(tuple(raw))
    while raw and raw[-1] in CORPORATE:
        raw.pop()
        if len(raw)>=2:result.append(tuple(raw))
    while len(raw)>2 and raw[-1] in {'fund','trust'}:
        raw.pop();result.append(tuple(raw))
    return tuple(dict.fromkeys(words(' '.join(phrase)) for phrase in result if len(phrase)>=2))

def matching_name(title_words,name):
    for phrase in variants(name):
        n=len(phrase)
        for i in range(len(title_words)-n+1):
            if title_words[i:i+n]!=phrase:continue
            tail=title_words[i+n:]
            # Do not conflate a base series with II/III even in fetch hints.
            suffix=list(tail)
            while suffix and suffix[0] in {'fund','trust','income','corporation','company','ltd'}:suffix.pop(0)
            if suffix and suffix[0] in SERIES and phrase[-1] not in SERIES:continue
            if suffix and suffix[0] in {'management','partners','holdings','advisory','advisers','advisors'}:continue
            if n>=3:return True
            # Short names are allowed only at the start of a financial release,
            # not inside prose or as the prefix of a different company name.
            if i!=0 or not tail:continue
            release=list(tail)
            while release and release[0] in {'fund','trust','income','corporation','company','ltd'}:release.pop(0)
            if not release or release[0] not in {'announces','announced','announce','declares','declared','declare','increases','increase','sets','reports','report','releases','release','notice','will','board','distribution','distributions','dividend','dividends','publishes'}:continue
            if FINANCIAL.search(' '.join(tail)):return True
    return False

@lru_cache(maxsize=1)
def reviewed_aliases():
    data=json.loads(ALIASES_PATH.read_text(encoding='utf-8'))
    assert data['schema']==1
    return data

def expanded_candidate_reason(title,context):
    names=getattr(context,'fund_names',{})
    sponsors=getattr(context,'sponsors',{})
    title_words=words(title);normalized=' '.join(title_words)
    for symbol,name in names.items():
        if symbol in context.known_symbols and matching_name(title_words,name):
            return 'registry_name_fetch:'+symbol
    for record in reviewed_aliases()['aliases']:
        symbol=record['symbol']
        if symbol not in context.known_symbols or names.get(symbol)!=record['registry_fund_name'] or sponsors.get(symbol)!=record['registry_sponsor']:continue
        if any(matching_name(title_words,name) for name in record['aliases']):
            return 'evidenced_name_fetch:'+symbol
    for record in reviewed_aliases().get('anonymous_titles',[]):
        symbol=record['guard_symbol']
        if (symbol in context.known_symbols and names.get(symbol)==record['registry_fund_name']
                and sponsors.get(symbol)==record['registry_sponsor'] and title_words==words(record['title'])):
            return 'reviewed_unnamed_distribution_fetch'
    for record in reviewed_aliases().get('governance_patterns',[]):
        symbol=record['guard_symbol']
        if (symbol in context.known_symbols and names.get(symbol)==record['registry_fund_name']
                and sponsors.get(symbol)==record['registry_sponsor']
                and {'shareholders','vote'}.issubset(title_words)
                and re.search(r'\bsub (?:adviser|advisor)\b',normalized)):
            return 'reviewed_fund_governance_fetch'
    # Names may precede or follow the action. Fund type + action + a current
    # sponsor are required; corporate earnings and ETF-only headlines stay out.
    closed_end='closed end' in normalized or 'closedend' in title_words
    if not ('funds' in title_words or (closed_end and 'fund' in title_words)) or not GROUP_EVENT.search(normalized):return None
    if re.search(r'\b(?:private equity|private credit|hedge funds?|interval funds?|fund management company)\b',normalized):return None
    if not closed_end and (re.search(r'\b(?:etf|etfs|open end|exchange traded|mutual funds?)\b',normalized)):return None
    brands=set()
    for symbol,profile in context.symbol_keywords.items():
        if symbol not in context.known_symbols:continue
        brands.update(words(p) for p in getattr(profile,'sponsor_phrases',()))
        brands.update(words(p) for p in getattr(profile,'sponsor_terms',()) if len(p)>=4 or p.isupper())
        sponsor=list(raw_words(sponsors.get(symbol,'')))
        while sponsor and sponsor[-1] in CORPORATE|{'lp','management','advisors','advisers','adviser','investment','investments','fund','funds','global','capital','asset','americas','usa'}:sponsor.pop()
        if sponsor:brands.add(tuple(REPLACEMENTS.get(w,w) for w in sponsor))
    for brand in brands:
        if not brand:continue
        if any(title_words[i:i+len(brand)]==brand for i in range(len(title_words)-len(brand)+1)):
            return 'sponsor_fund_event_fetch'
    return None

def load_selection_context(db):
    """Fresh names supplement the existing profile; they do not change extraction."""
    from sqlalchemy import select
    from app.models import Ticker
    from app.ticker_context import load_ticker_context
    base=load_ticker_context(db)
    rows=db.execute(select(Ticker.symbol,Ticker.fund_name,Ticker.sponsor).where(Ticker.active.is_(True))).all()
    return SimpleNamespace(known_symbols=base.known_symbols,symbol_keywords=base.symbol_keywords,
        fund_names={r[0].upper():r[1] for r in rows},sponsors={r[0].upper():r[2] for r in rows})
