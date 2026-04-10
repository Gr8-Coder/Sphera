from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Dict, Iterable, List, Optional, Tuple

from app.schemas import ClassifiedNewsItem, RawNewsItem


@dataclass(frozen=True)
class ThemeRule:
    slug: str
    label: str
    trigger_terms: Tuple[str, ...]
    positive_signal_terms: Tuple[str, ...]
    negative_signal_terms: Tuple[str, ...]


TRACKED_THEMES: Tuple[ThemeRule, ...] = (
    ThemeRule(
        slug="distributor_expansion",
        label="Distributor expansion",
        trigger_terms=(
            "distributor",
            "distribution network",
            "distribution reach",
            "stockist",
            "super stockist",
            "appointed distributors",
            "distribution footprint",
        ),
        positive_signal_terms=(
            "expand",
            "expansion",
            "add distributors",
            "appoint",
            "onboard",
            "launch",
            "rollout",
            "new market",
            "network growth",
            "broaden reach",
            "scale distribution",
            "ramp",
        ),
        negative_signal_terms=(
            "rationalise distribution",
            "rationalize distribution",
            "reduce distributors",
            "exit market",
            "shut",
            "close",
            "terminate distributors",
            "consolidate channel",
        ),
    ),
    ThemeRule(
        slug="dealer_financing_gaps",
        label="Dealer financing gaps",
        trigger_terms=(
            "dealer finance",
            "dealer financing",
            "dealer credit",
            "channel finance",
            "retail finance",
            "dealer liquidity",
            "dealer funding",
            "credit to dealers",
        ),
        positive_signal_terms=(
            "gap",
            "liquidity stress",
            "credit crunch",
            "cash crunch",
            "tight liquidity",
            "financing need",
            "delayed payments",
            "overdues",
            "slow collections",
            "higher receivables",
        ),
        negative_signal_terms=(
            "ample liquidity",
            "easy financing",
            "improved collections",
            "low overdues",
            "healthy dealer cash flow",
            "strong dealer balance sheet",
            "collections improved",
        ),
    ),
    ThemeRule(
        slug="working_capital_commentary",
        label="Working capital commentary",
        trigger_terms=(
            "working capital",
            "receivables",
            "inventory days",
            "cash conversion cycle",
            "payables cycle",
            "liquidity",
            "cash flow",
            "operating cash",
        ),
        positive_signal_terms=(
            "pressure",
            "tight",
            "stretched",
            "elongated",
            "slower collections",
            "receivables buildup",
            "inventory build-up",
            "higher debtor days",
            "cash flow stress",
            "working capital need",
            "strain",
        ),
        negative_signal_terms=(
            "improved working capital",
            "reduced receivables",
            "shorter cash conversion cycle",
            "healthy operating cash flow",
            "better collections",
            "lower inventory",
            "cash conversion improved",
        ),
    ),
    ThemeRule(
        slug="channel_scale_up",
        label="Channel scale-up",
        trigger_terms=(
            "channel partner",
            "channel expansion",
            "dealer network",
            "trade channel",
            "general trade",
            "modern trade",
            "retail footprint",
            "sales channel",
        ),
        positive_signal_terms=(
            "scale up",
            "scale-up",
            "expand network",
            "add outlets",
            "add dealers",
            "increase reach",
            "strengthen channel",
            "channel ramp-up",
            "aggressive expansion",
            "ramp",
        ),
        negative_signal_terms=(
            "channel rationalisation",
            "channel rationalization",
            "fewer dealers",
            "direct-to-consumer shift",
            "reduce channel partners",
            "channel contraction",
        ),
    ),
    ThemeRule(
        slug="rural_secondary_distribution_growth",
        label="Rural/secondary distribution growth",
        trigger_terms=(
            "rural",
            "rural distribution",
            "semi-urban",
            "tier 2",
            "tier 3",
            "upcountry",
            "secondary distribution",
            "secondary sales",
            "secondary market",
        ),
        positive_signal_terms=(
            "deeper rural reach",
            "rural expansion",
            "rural growth",
            "rural distribution",
            "penetration",
            "new outlets",
            "distribution push",
            "secondary sales growth",
            "tier 2 growth",
            "tier 3 growth",
            "ramp",
            "momentum",
        ),
        negative_signal_terms=(
            "weak rural demand",
            "rural slowdown",
            "pullback",
            "distribution cutback",
            "lower secondary sales",
            "tier 2 slowdown",
            "tier 3 slowdown",
        ),
    ),
)

THEME_LABELS: Dict[str, str] = {theme.slug: theme.label for theme in TRACKED_THEMES}
GLOBAL_POSITIVE_TERMS = (
    "expand",
    "expansion",
    "scale",
    "growth",
    "grow",
    "launch",
    "appoint",
    "add",
    "ramp",
    "penetration",
    "deepen",
    "broad",
    "stress",
    "tight",
    "gap",
    "need",
    "momentum",
)
GLOBAL_NEGATIVE_TERMS = (
    "reduce",
    "rationalise",
    "rationalize",
    "cutback",
    "improve",
    "healthy",
    "lower",
    "shorter",
    "exit",
    "close",
    "slowdown",
)


class CredServClassifier:
    def classify(self, item: RawNewsItem) -> Optional[ClassifiedNewsItem]:
        text = f"{item.title}. {item.snippet or ''}".lower()
        theme_scores: List[Tuple[float, ThemeRule, List[str], float, float]] = []

        for rule in TRACKED_THEMES:
            trigger_score, trigger_matches = self._score_terms(text, rule.trigger_terms, 1.5)
            if trigger_score <= 0:
                continue

            positive_score, positive_matches = self._score_terms(
                text, rule.positive_signal_terms, 1.25
            )
            negative_score, negative_matches = self._score_terms(
                text, rule.negative_signal_terms, 1.25
            )

            if positive_score == 0:
                positive_boost, global_positive_matches = self._score_terms(
                    text, GLOBAL_POSITIVE_TERMS, 0.5
                )
                positive_score += positive_boost
                positive_matches.extend(global_positive_matches)
            if negative_score == 0:
                negative_boost, global_negative_matches = self._score_terms(
                    text, GLOBAL_NEGATIVE_TERMS, 0.5
                )
                negative_score += negative_boost
                negative_matches.extend(global_negative_matches)

            score = trigger_score + max(positive_score, negative_score)
            matched_terms = trigger_matches + positive_matches + negative_matches
            theme_scores.append(
                (score, rule, matched_terms, positive_score, negative_score)
            )

        if not theme_scores:
            return None

        best_score, best_rule, matched_terms, positive_score, negative_score = max(
            theme_scores, key=lambda item_score: item_score[0]
        )
        if best_score < 2:
            return None

        signal = "positive" if positive_score >= negative_score else "negative"
        if positive_score == 0 and negative_score == 0:
            return None

        confidence = min(0.96, 0.48 + (best_score * 0.08))
        rationale_terms = ", ".join(dict.fromkeys(matched_terms[:6]))
        signal_reason = (
            f"Matched {best_rule.label} because the snippet referenced {rationale_terms}. "
            f"Signal is {signal} for CredServ propensity."
        )

        return ClassifiedNewsItem(
            company_name=item.company_name,
            title=item.title,
            snippet=item.snippet,
            source_name=item.source_name,
            source_url=item.source_url,
            published_at=item.published_at,
            raw_summary=item.raw_summary,
            theme_slug=best_rule.slug,
            theme_label=best_rule.label,
            signal=signal,
            signal_reason=signal_reason,
            confidence=round(confidence, 2),
        )

    @staticmethod
    def _score_terms(
        text: str, terms: Iterable[str], weight: float
    ) -> Tuple[float, List[str]]:
        score = 0.0
        matches = []
        for term in terms:
            if term in text:
                score += weight + (0.2 if " " in term or "-" in term else 0.0)
                matches.append(term)
        return score, matches


def build_item_hash(item: ClassifiedNewsItem) -> str:
    published_at = item.published_at.isoformat() if item.published_at else "na"
    payload = "||".join(
        [
            item.company_name.lower(),
            item.title.lower(),
            item.source_url.lower(),
            published_at,
        ]
    )
    return sha256(payload.encode("utf-8")).hexdigest()
