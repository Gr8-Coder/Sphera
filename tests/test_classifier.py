from datetime import datetime, timezone

from app.schemas import RawNewsItem
from app.services.classifier import CredServClassifier


def test_classifier_detects_distributor_expansion_positive_signal():
    classifier = CredServClassifier()
    item = RawNewsItem(
        company_name="Acme Foods",
        title="Acme Foods appoints 120 new distributors to expand rural reach",
        snippet="The company said the distribution network expansion will deepen penetration in tier 2 and tier 3 markets.",
        source_name="Example News",
        source_url="https://example.com/news/1",
        published_at=datetime.now(timezone.utc),
    )

    result = classifier.classify(item)

    assert result is not None
    assert result.theme_slug == "distributor_expansion"
    assert result.signal == "positive"


def test_classifier_detects_working_capital_improvement_as_negative_propensity():
    classifier = CredServClassifier()
    item = RawNewsItem(
        company_name="Bravo Limited",
        title="Bravo Limited reports improved working capital and lower receivables",
        snippet="Management said the cash conversion cycle shortened materially because collections improved.",
        source_name="Example News",
        source_url="https://example.com/news/2",
        published_at=datetime.now(timezone.utc),
    )

    result = classifier.classify(item)

    assert result is not None
    assert result.theme_slug == "working_capital_commentary"
    assert result.signal == "negative"


def test_classifier_detects_rural_distribution_growth_positive_signal():
    classifier = CredServClassifier()
    item = RawNewsItem(
        company_name="Adani Wilmar",
        title="Adani Wilmar ramps up rural distribution as food and FMCG business gains momentum",
        snippet="Management said the company is deepening reach in tier 2 and tier 3 markets.",
        source_name="Example News",
        source_url="https://example.com/news/3",
        published_at=datetime.now(timezone.utc),
    )

    result = classifier.classify(item)

    assert result is not None
    assert result.theme_slug == "rural_secondary_distribution_growth"
    assert result.signal == "positive"
