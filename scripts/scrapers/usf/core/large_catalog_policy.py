from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryDecision:
    status: str
    delay_hours: int
    reason: str


def content_miss_retry(miss_count: int) -> RetryDecision:
    """
    Planning na een inhoudelijk geldige detailcontrole zonder EAN.

    Poging 1  -> 1 dag
    Poging 2  -> 7 dagen
    Poging 3  -> 90 dagen
    Poging 4  -> 180 dagen
    Poging 5+ -> 365 dagen
    """
    if miss_count < 1:
        raise ValueError("miss_count moet minimaal 1 zijn")

    if miss_count == 1:
        return RetryDecision(
            status="not_found",
            delay_hours=24,
            reason="content_miss_retry_1d",
        )

    if miss_count == 2:
        return RetryDecision(
            status="not_found",
            delay_hours=7 * 24,
            reason="content_miss_retry_7d",
        )

    if miss_count == 3:
        return RetryDecision(
            status="not_found",
            delay_hours=90 * 24,
            reason="content_miss_pause_90d",
        )

    if miss_count == 4:
        return RetryDecision(
            status="not_found",
            delay_hours=180 * 24,
            reason="content_miss_pause_180d",
        )

    return RetryDecision(
        status="not_found",
        delay_hours=365 * 24,
        reason="content_miss_annual_recheck",
    )


def technical_failure_retry(failure_count: int) -> RetryDecision:
    """
    Technische fouten tellen niet als inhoudelijke EAN-misser.

    Fout 1 -> 6 uur
    Fout 2 -> 24 uur
    Fout 3 -> 72 uur
    Fout 4+ -> 7 dagen
    """
    if failure_count < 1:
        raise ValueError("failure_count moet minimaal 1 zijn")

    if failure_count == 1:
        delay_hours = 6
    elif failure_count == 2:
        delay_hours = 24
    elif failure_count == 3:
        delay_hours = 72
    else:
        delay_hours = 7 * 24

    return RetryDecision(
        status="technical_error",
        delay_hours=delay_hours,
        reason=f"technical_retry_{delay_hours}h",
    )
