# degraded_mode.py
# Stateless fast-path for circuit-breaker degraded mode (Friction Policy).

from __future__ import annotations
import json

print("[TRADE_RISK_V2_FC] Loading degraded_mode.py")

def evaluate_trade_fastpath(features, user_code, txn_id):
    """
    Stateless fast-path evaluator when AST rules DB path is unavailable.
    PRD Mandate: Apply 'Friction' (block high-leverage), but allow normal spot/low-leverage trading.
    """
    safe_features = features or {}
    
    # Safely extract leverage (could be in features dict or raw payload)
    leverage = 0
    try:
        if "current_leverage" in safe_features:
            leverage = int(safe_features.get("current_leverage", 0))
    except (ValueError, TypeError):
        leverage = 0

    # Friction Rule: Block >= 50x leverage during brownouts
    if leverage >= 50:
        return {
            "triggered": True,
            "decision": "HOLD",
            "reason": "DEGRADED_HIGH_LEVERAGE_BLOCK",
            "rule_name": "DEGRADED_MODE_FRICTION",
            "alert_type": "Degraded Mode Block",
            "narrative": f"[Degraded Fast-Path] Blocked {leverage}x leverage trade during DB brownout.",
            "enforcement_actions": ["FUTURES_TRADE_BAN"],
        }
    
    # Fail-open for Spot and low-leverage Futures to protect market liquidity
    return {
        "triggered": False,
        "decision": "PASS",
        "reason": "DEGRADED_SAFE_PASS",
        "rule_name": "DEGRADED_MODE_FRICTION",
        "alert_type": "Degraded Mode Pass",
        "narrative": f"[Degraded Fast-Path] Passed low-risk trade ({leverage}x leverage).",
        "enforcement_actions": [],
    }
