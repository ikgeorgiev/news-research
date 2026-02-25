from app.ingestion import _clamp_label, _extract_entry_tickers


def test_extract_tickers_from_context_and_text():
    known = {"GOF", "UTF", "AAPL"}
    title = "Fund update for (GOF) and NYSE: UTF"
    summary = "The manager also mentioned AAPL in a comparison section."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF,UTF"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert "GOF" in hits
    assert "UTF" in hits
    assert "AAPL" in hits
    assert hits["GOF"][1] >= 0.75


def test_multi_symbol_context_does_not_mass_assign():
    known = {"GOF", "UTF", "PDI"}
    title = "GOF monthly update"
    summary = "Distribution policy unchanged."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF,UTF,PDI"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert "GOF" in hits
    assert "UTF" not in hits
    assert "PDI" not in hits


def test_single_symbol_context_is_allowed():
    known = {"GOF", "UTF"}
    title = "Monthly portfolio commentary"
    summary = "No explicit symbol in text."
    link = "https://example.com/story"
    feed_url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=GOF"

    hits = _extract_entry_tickers(title, summary, link, feed_url, known)

    assert hits.get("GOF") == ("context", 0.93)


def test_clamp_label_caps_to_column_width():
    long_text = "X" * 240
    clipped = _clamp_label(long_text)
    assert len(clipped) == 120
