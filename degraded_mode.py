# degraded_mode.py
# Stateless fast-path for circuit-breaker degraded mode (exposure cap in next wave).

from __future__ import annotations

print("[TRADE_RISK_V2_FC] Loading degraded_mode.py")


def evaluate_trade_fastpath(features, user_code, txn_id):
    """
    Placeholder fast-path evaluator when AST rules DB path is unavailable.
    Exposure-cap logic will be wired in the next implementation wave.
    """
    return {
        "triggered": True,
        "decision": "HOLD",
        "reason": "SYSTEM_DEGRADED_FASTPATH",
        "rule_name": "CIRCUIT_BREAKER_DEGRADED",
        "alert_type": "Degraded Mode",
        "narrative": (
            f"[Degraded Fast-Path] AST rules skipped for user={user_code}, txn={txn_id}. "
            "Exposure-cap evaluation pending."
        ),
        "enforcement_actions": [],
    }
