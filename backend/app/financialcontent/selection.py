"""Reviewed fetch triage only; never ticker assignment."""
import re

# Reviewed against Oct 28 wire bodies and the current Oct 29 ticker registry.
# Evidence: field-study/virtus-review-20261028/database-validation.json.
# These names authorize a body fetch only, never a ticker assignment or commit.
# The registry phrase guard prevents reusing an alias for a changed fund identity.
REVIEWED_FUND_TITLE_ALIASES = {
    'ACV': ('virtus diversified inc conv fund', (
        'virtus diversified income convertible fund',
        'virtus diversified income and convertible fund',
    )),
    'AIO': ('virtus ai tech opportunity fund', (
        'virtus artificial intelligence technology opportunities fund',
        'virtus artificial intelligence and technology opportunities fund',
    )),
    # Oct29 saved wire summary explicitly identifies NYSE:EIC; registry uses Co.
    'EIC': ('eagle point income co', ('eagle point income company',)),
}


def _existing_candidate_reason(title, ticker_context):
    """Broad fetch triage only. App extraction still validates the article body."""
    # The saved Oct30 Banesco body identifies a bank, not Liberty All-Star.
    # Remove only USA inside this reviewed issuer name from symbol triage.
    # A separate USA token, other symbols and full-name matching still apply.
    # This is body-fetch triage, never ticker assignment or a general USA ban.
    symbol_title = re.sub(r'\bBanesco\s+USA\b', 'Banesco', title, flags=re.IGNORECASE)
    # The saved Nov3 original BW body identifies USA Compression Partners, LP
    # as USAC. Ignore only USA within this exact reviewed corporate name;
    # separate USA labels and all other fund evidence remain eligible.
    symbol_title = re.sub(r'\bUSA\s+Compression\s+Partners\b', 'Compression Partners', symbol_title, flags=re.IGNORECASE)
    # Nov3 saved wire bodies identify a medical award and the cycling body,
    # not the USA fund. Mask only these exact reviewed name phrases.
    symbol_title = re.sub(r'\bPrix\s+Galien\s+USA\b', 'Prix Galien', symbol_title, flags=re.IGNORECASE)
    symbol_title = re.sub(r'\bUSA\s+Cycling\b', 'Cycling', symbol_title, flags=re.IGNORECASE)
    # The saved Nov 5 bodies identify the TDAY issuer, Kontakt.io's KIO Clinic
    # product and Celanese's automotive award. Keep every other token and run
    # full-name/sponsor matching below against the original headline.
    symbol_title = re.sub(r'\bUSA(?=\s+TODAY\s+Co\.(?!\w))', '', symbol_title, flags=re.IGNORECASE)
    # HomeFree-USA is the housing nonprofit named in the saved VantageScore release.
    symbol_title = re.sub(r'\bHomeFree[-\u2010\u2011]USA\b', 'HomeFree', symbol_title, flags=re.IGNORECASE)
    symbol_title = re.sub(r'(\bKontakt\.io\s+Introduces\s+)KIO(?=\s+Clinic\b)', r'\1', symbol_title, flags=re.IGNORECASE)
    if re.search(r'\bCelanese\b', title, flags=re.IGNORECASE):
        symbol_title = re.sub(r'\bSPE(?=Â®\s+Automotive\s+Innovation\s+Awards?\b)', '', symbol_title, flags=re.IGNORECASE)
    words = set(re.findall(r'\b[A-Z][A-Z0-9]{1,9}\b', symbol_title))
    # An explicit exchange label remains body-fetch evidence even if the words
    # that follow it collide with a reviewed name. Ticker assignment is later.
    if re.search(r'\b(?:NYSE(?:\s+American)?|NASDAQ(?:GS|GM|CM)?|AMEX)\s*:\s*USA\b', title, flags=re.IGNORECASE):
        words.add('USA')
    # An uppercase symbol explicitly labelled on an exchange still counts,
    # even when its following words match a reviewed product or award.
    words.update(re.findall(r'(?i:\b(?:NYSE(?:\s+American)?|NASDAQ(?:GS|GM|CM)?|AMEX)\s*:\s*)(KIO|SPE)\b', title))
    symbols = words & set(ticker_context.known_symbols)
    if symbols:
        return 'title_symbol:' + ','.join(sorted(symbols))
    normalized = ' ' + ' '.join(re.findall(r'\w+', title.casefold())) + ' '
    for symbol, profile in ticker_context.symbol_keywords.items():
        for phrase in profile.fund_title_phrases:
            if len(phrase.split()) >= 2 and ' ' + phrase + ' ' in normalized:
                return 'fund_title:' + symbol
    for symbol, (registry_phrase, aliases) in REVIEWED_FUND_TITLE_ALIASES.items():
        profile = ticker_context.symbol_keywords.get(symbol)
        if symbol not in ticker_context.known_symbols or profile is None:
            continue
        if registry_phrase not in profile.fund_title_phrases:
            continue
        if any(' ' + alias + ' ' in normalized for alias in aliases):
            return 'reviewed_fund_alias:' + symbol
    # Source Capital's two-word name is deliberately absent from the app's
    # general title matcher. Keep this research-only exception narrow: exact
    # current profile, active SOR, leading name and a distribution announcement.
    sor = ticker_context.symbol_keywords.get('SOR')
    if ('SOR' in ticker_context.known_symbols and sor is not None
            and not sor.fund_title_phrases
            and getattr(sor, 'fund_terms', ()) == frozenset({'source'})
            and getattr(sor, 'fund_phrases', ()) == frozenset({'source capital'})
            and re.match(r' source capital (?:increases|declares|announces) (?:monthly |regular |quarterly )?distribution\b', normalized)):
        return 'reviewed_fund_alias:SOR'
    # Group announcements omit individual fund names. Require explicit closed-
    # end funds, a distribution declaration, and a leading current sponsor.
    # This authorizes a body read only; the reason intentionally assigns no ticker.
    if re.search(r'\bclosed end funds? (?:declare|declares|announce|announces) (?:monthly |quarterly |regular )?(?:distributions?|dividends?)\b', normalized):
        for symbol, profile in ticker_context.symbol_keywords.items():
            if symbol not in ticker_context.known_symbols:
                continue
            brands = set(getattr(profile, 'sponsor_phrases', ()))
            brands.update(term for term in getattr(profile, 'sponsor_terms', ()) if len(term) >= 4)
            if any(normalized.startswith(' ' + brand + ' ') for brand in brands):
                return 'sponsor_closed_end_distribution'
    return None


def candidate_reason(title,ticker_context):
    existing=_existing_candidate_reason(title,ticker_context)
    if existing:return existing
    from app.financialcontent.title_names import expanded_candidate_reason
    return expanded_candidate_reason(title,ticker_context)
