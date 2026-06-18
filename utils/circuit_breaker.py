# utils/circuit_breaker.py
# Per-instance FC circuit breaker with out-of-band DB recovery probes.

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, TypeVar

print("[TRADE_RISK_V2_FC] Loading utils/circuit_breaker.py")

STATE_CLOSED = "CLOSED"
STATE_OPEN = "OPEN"

FAILURE_THRESHOLD = 3
OPEN_COOLDOWN = 30.0
PROBE_TIMEOUT_S = 0.5
DB_OP_TIMEOUT_S = 3.0

FAST_PATH_TRIGGER = object()

_STATE = STATE_CLOSED
_FAIL_COUNT = 0
_LAST_FAILURE_TIME = 0.0
_NEXT_PROBE_TIME = 0.0

T = TypeVar("T")


def reset_circuit_breaker() -> None:
    """Reset module state (test helper)."""
    global _STATE, _FAIL_COUNT, _LAST_FAILURE_TIME, _NEXT_PROBE_TIME
    _STATE = STATE_CLOSED
    _FAIL_COUNT = 0
    _LAST_FAILURE_TIME = 0.0
    _NEXT_PROBE_TIME = 0.0


def _is_db_failure(exc: BaseException) -> bool:
    try:
        import psycopg2

        if isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            return True
    except ImportError:
        pass

    return isinstance(
        exc,
        (
            TimeoutError,
            asyncio.TimeoutError,
            ConnectionError,
            OSError,
        ),
    )


class CircuitBreaker:
    """Instance-level breaker backed by module globals (warm FC reuse)."""

    @property
    def state(self) -> str:
        return _STATE

    @property
    def fail_count(self) -> int:
        return _FAIL_COUNT

    def is_open(self) -> bool:
        return _STATE == STATE_OPEN

    def is_closed(self) -> bool:
        return _STATE == STATE_CLOSED

    async def execute_with_breaker(
        self,
        db_func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T | object:
        global _STATE, _FAIL_COUNT, _LAST_FAILURE_TIME, _NEXT_PROBE_TIME

        if _STATE == STATE_CLOSED:
            return await self._execute_closed(db_func, *args, **kwargs)

        now = time.monotonic()
        if now < _NEXT_PROBE_TIME:
            print(
                "[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] OPEN — rejecting DB call "
                f"(probe in {(_NEXT_PROBE_TIME - now):.1f}s)"
            )
            return FAST_PATH_TRIGGER

        if not await self._probe_db_health():
            _STATE = STATE_OPEN
            _NEXT_PROBE_TIME = time.monotonic() + OPEN_COOLDOWN
            print(
                "[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] Out-of-band probe failed — "
                f"remaining OPEN for {OPEN_COOLDOWN}s"
            )
            return FAST_PATH_TRIGGER

        _transition_to_closed()
        return await self._execute_closed(db_func, *args, **kwargs)

    async def _execute_closed(
        self,
        db_func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T | object:
        global _STATE, _FAIL_COUNT, _LAST_FAILURE_TIME, _NEXT_PROBE_TIME

        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(db_func, *args, **kwargs),
                timeout=DB_OP_TIMEOUT_S,
            )
        except Exception as exc:
            if not _is_db_failure(exc):
                raise

            _FAIL_COUNT += 1
            _LAST_FAILURE_TIME = time.monotonic()
            print(
                "[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] DB failure "
                f"{_FAIL_COUNT}/{FAILURE_THRESHOLD}: {exc}"
            )

            if _FAIL_COUNT >= FAILURE_THRESHOLD:
                _STATE = STATE_OPEN
                _NEXT_PROBE_TIME = time.monotonic() + OPEN_COOLDOWN
                _log_critical_open()
                return FAST_PATH_TRIGGER

            raise

        _FAIL_COUNT = 0
        return result

    async def _probe_db_health(self) -> bool:
        import core

        try:
            await asyncio.wait_for(
                asyncio.to_thread(core.ping_db),
                timeout=PROBE_TIMEOUT_S,
            )
            print("[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] Out-of-band probe succeeded")
            return True
        except Exception as exc:
            print(f"[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] Out-of-band probe failed: {exc}")
            return False


def _transition_to_closed() -> None:
    global _STATE, _FAIL_COUNT, _LAST_FAILURE_TIME, _NEXT_PROBE_TIME
    previous = _STATE
    _STATE = STATE_CLOSED
    _FAIL_COUNT = 0
    _LAST_FAILURE_TIME = 0.0
    _NEXT_PROBE_TIME = 0.0
    if previous != STATE_CLOSED:
        print(
            "[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] CRITICAL RECOVERY — "
            "transitioned OPEN -> CLOSED after successful probe"
        )


def _log_critical_open() -> None:
    print(
        "[TRADE_RISK_V2_FC][CIRCUIT_BREAKER] *** CRITICAL ALERT *** "
        f"DB failures reached {FAILURE_THRESHOLD}; breaker OPEN for {OPEN_COOLDOWN}s"
    )


_breaker = CircuitBreaker()


def get_breaker() -> CircuitBreaker:
    return _breaker


def run_with_breaker(db_func: Callable[..., T], *args: Any, **kwargs: Any) -> T | object:
    """Sync wrapper for FC handlers invoking async breaker execution."""
    return asyncio.run(get_breaker().execute_with_breaker(db_func, *args, **kwargs))
