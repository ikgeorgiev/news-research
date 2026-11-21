import pytest
from prometheus_client import REGISTRY

from app.monitoring import observe_article_commit


@pytest.mark.parametrize("kind", ["feed_response", "pending_enrichment"])
def test_article_metrics_keep_cohorts_and_convert_milliseconds(kind):
    labels = {"source": "other", "discovery_kind": kind}
    counter = "news_articles_committed_total"
    before = REGISTRY.get_sample_value(counter, labels)
    stage_labels = {**labels, "stage": "receipt_to_commit"}
    before_sum = REGISTRY.get_sample_value("news_article_stage_seconds_sum", stage_labels)
    before_count = REGISTRY.get_sample_value("news_article_stage_seconds_count", stage_labels)
    observe_article_commit({
        "source": "arbitrary-provider-identifier", "discovery_kind": kind,
        "receipt_to_commit_ms": 1250, "article_url": "https://example.test/private-id",
    })
    assert REGISTRY.get_sample_value(counter, labels) == before + 1
    assert REGISTRY.get_sample_value("news_article_stage_seconds_sum", stage_labels) == pytest.approx(before_sum + 1.25)
    assert REGISTRY.get_sample_value("news_article_stage_seconds_count", stage_labels) == before_count + 1
    for metric in REGISTRY.collect():
        if metric.name.startswith("news_"):
            assert all("article_url" not in sample.labels for sample in metric.samples)
            assert all(sample.labels.get("source") != "arbitrary-provider-identifier" for sample in metric.samples)


def test_invalid_stage_values_do_not_poison_histograms():
    labels = {"source": "other", "discovery_kind": "feed_response", "stage": "commit"}
    before = REGISTRY.get_sample_value("news_article_stage_seconds_count", labels)
    for value in (float("nan"), float("inf"), -1, None):
        observe_article_commit({"source": "other", "discovery_kind": "feed_response", "commit_ms": value})
    assert REGISTRY.get_sample_value("news_article_stage_seconds_count", labels) == before
