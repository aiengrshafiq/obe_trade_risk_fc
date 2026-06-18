# utils/kill_switch.py
# Bounded-cache OTS reader for phalanx_global_state (fail-closed kill switch).

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Callable, TypeVar

print("[TRADE_RISK_V2_FC] Loading utils/kill_switch.py")

CACHE_TTL = 5.0
OTS_FETCH_TIMEOUT_S = 0.5
OTS_MAX_RETRIES = 2

PHALANX_GLOBAL_STATE_TABLE = "phalanx_global_state"
GLOBAL_STATE_KEY = "GLOBAL_KILL_SWITCH"
ACTION_COLUMN = "action"
ENGAGED_COLUMN = "engaged"

ACTION_HOLD = "HOLD"
ACTION_NORMAL = "NORMAL"

_last_known_good_state: str | None = None
_last_fetch_time: float = 0.0

_ots_client = None
_executor = ThreadPoolExecutor(max_workers=1)

T = TypeVar("T")


def reset_kill_switch_cache() -> None:
    """Reset module cache (test helper)."""
    global _last_known_good_state, _last_fetch_time
    _last_known_good_state = None
    _last_fetch_time = 0.0


def get_kill_switch_action() -> str:
    """
    Return ACTION_HOLD or ACTION_NORMAL.

    Fail-closed: returns ACTION_HOLD when OTS is unreachable and cache is expired/empty.
    Fail-safe: returns cached state on transient OTS errors while cache is within TTL.
    """
    global _last_known_good_state, _last_fetch_time

    now = time.monotonic()
    if _last_known_good_state is not None and (now - _last_fetch_time) < CACHE_TTL:
        return _last_known_good_state

    fetched = _fetch_state_with_retries()
    if fetched is not None:
        _last_known_good_state = fetched
        _last_fetch_time = time.monotonic()
        return fetched

    if _last_known_good_state is not None and (time.monotonic() - _last_fetch_time) < CACHE_TTL:
        return _last_known_good_state

    return ACTION_HOLD


def _fetch_state_with_retries() -> str | None:
    for _ in range(OTS_MAX_RETRIES + 1):
        try:
            return _fetch_state_from_ots()
        except Exception:
            continue
    return None


def _fetch_state_from_ots() -> str:
    return _run_with_timeout(_read_global_state_row, OTS_FETCH_TIMEOUT_S)


def _run_with_timeout(func: Callable[[], T], timeout_s: float) -> T:
    future = _executor.submit(func)
    try:
        return future.result(timeout=timeout_s)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise TimeoutError(f"OTS fetch exceeded {timeout_s}s") from exc


def _get_ots_client():
    global _ots_client
    if _ots_client is not None:
        return _ots_client

    endpoint = os.environ.get("OTS_ENDPOINT")
    instance_name = os.environ.get("OTS_INSTANCE")
    access_key_id = os.environ.get("OTS_ACCESS_KEY_ID")
    access_key_secret = os.environ.get("OTS_ACCESS_KEY_SECRET")
    missing = [
        name
        for name, value in (
            ("OTS_ENDPOINT", endpoint),
            ("OTS_INSTANCE", instance_name),
            ("OTS_ACCESS_KEY_ID", access_key_id),
            ("OTS_ACCESS_KEY_SECRET", access_key_secret),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing required OTS environment variables: {', '.join(missing)}"
        )

    from tablestore import OTSClient

    _ots_client = OTSClient(
        endpoint,
        access_key_id,
        access_key_secret,
        instance_name,
    )
    return _ots_client


def _read_global_state_row() -> str:
    client = _get_ots_client()
    primary_key = [("config_key", GLOBAL_STATE_KEY)]
    _consumed, row, _next_token = client.get_row(
        PHALANX_GLOBAL_STATE_TABLE,
        primary_key,
        [ACTION_COLUMN, ENGAGED_COLUMN],
    )
    return _normalize_row_state(row)


def _normalize_row_state(row) -> str:
    if row is None or not row.attribute_columns:
        return ACTION_NORMAL

    action_value = None
    engaged_value = None
    for name, value, _timestamp in row.attribute_columns:
        if name == ACTION_COLUMN:
            action_value = value
        elif name == ENGAGED_COLUMN:
            engaged_value = value

    if action_value is not None:
        action = str(action_value).strip().upper()
        if action == ACTION_HOLD:
            return ACTION_HOLD
        if action == ACTION_NORMAL:
            return ACTION_NORMAL

    if engaged_value is not None and bool(engaged_value):
        return ACTION_HOLD

    return ACTION_NORMAL
