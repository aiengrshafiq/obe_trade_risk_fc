import json
import base64
import time
import core
import degraded_mode
from utils import kill_switch
from utils import circuit_breaker

print("[TRADE_RISK_V2_FC] System initializing - Trade Risk Pipeline V2")

def _make_response(status_code, data):
    return {
        "statusCode": status_code,
        "headers": {"content-type": "application/json"},
        "body": json.dumps(data, default=str),
        "isBase64Encoded": False,
    }


def _wait_for_features_with_breaker(user_code, txn_id, max_retries, delay):
    """Poll features through the circuit breaker; returns FAST_PATH_TRIGGER on OPEN."""
    for attempt in range(max_retries):
        try:
            result = circuit_breaker.run_with_breaker(
                core.fetch_trade_features_strict,
                user_code,
                txn_id,
            )
        except Exception as exc:
            # print(
            #     f"[TRADE_RISK_V2_FC] Breaker-wrapped feature fetch error "
            #     f"(attempt {attempt + 1}/{max_retries}): {exc}"
            # )
            time.sleep(delay)
            continue
        if result is circuit_breaker.FAST_PATH_TRIGGER:
            return circuit_breaker.FAST_PATH_TRIGGER
        if result:
            return result
        # print(
        #     f"[TRADE_RISK_V2_FC] Feature not ready, attempt {attempt + 1}/{max_retries}"
        # )
        time.sleep(delay)
    return None


def _handle_degraded_fast_path(user_code, txn_id_input, features, is_api_request):
    """Route to fast-path when breaker is OPEN — no AST rules, no enforcement side effects."""
    safe_features = features or {
        "user_code": str(user_code),
        "txn_id": str(txn_id_input),
    }
    rule_result = degraded_mode.evaluate_trade_fastpath(
        safe_features, user_code, txn_id_input
    )
    # print(f"[TRADE_RISK_V2_FC] Degraded fast-path result: {rule_result}")
    return _make_response(
        200,
        {
            "user_code": str(user_code),
            "txn_id": str(txn_id_input),
            "decision": rule_result.get("decision", "HOLD"),
            "reason": rule_result.get("reason", "SYSTEM_DEGRADED_FASTPATH"),
            "rule_hit": rule_result.get("rule_name"),
            "alert_type": rule_result.get("alert_type"),
            "narrative": rule_result.get("narrative"),
            "degraded_mode": True,
        },
    )

def handler(event, context):

    
    
    # Step 1: Kill Switch — highest precedence, abort all side effects.
    if kill_switch.get_kill_switch_action() == kill_switch.ACTION_HOLD:
        # print("[TRADE_RISK_V2_FC] GLOBAL KILL SWITCH ENGAGED — Risk Engine standing down.")
        return _make_response(200, {
            "user_code": str(user_code) if 'user_code' in locals() else "UNKNOWN",
            "txn_id": str(txn_id_input) if 'txn_id_input' in locals() else "UNKNOWN",
            "decision": "PASS", # CRITICAL: Fail-open to protect exchange liquidity
            "reason": "RISK_ENGINE_KILLED",
            "message": "Enforcement halted due to Global Kill Switch.",
        })

    # Step 2: Circuit Breaker state check (per warm FC instance).
    breaker = circuit_breaker.get_breaker()
    # print(f"[TRADE_RISK_V2_FC] Circuit breaker state: {breaker.state}")

    start_ts     = time.perf_counter()
    user_code    = None
    txn_id_input = None
    is_api_request = False

    print(f"[TRADE_RISK_V2_FC] Handler invoked.")

    # ==========================
    # 1. Parse Event (Kafka / HTTP API)
    # ==========================
    try:
        event_str = (
            event.decode("utf-8", errors="ignore")
            if isinstance(event, (bytes, bytearray))
            else (event if isinstance(event, str) else json.dumps(event))
        )
        # print(f"[TRADE_RISK_V2_FC] Raw Event (first 500): {event_str[:500]}")
        envelope = json.loads(event_str)

        # ---- Kafka Trigger ----
        if (
            isinstance(envelope, list)
            and len(envelope) > 0
            and isinstance(envelope[0], dict)
            and "value" in envelope[0]
        ):
            rec     = envelope[0]
            raw_val = rec.get("value")

            # Parse canal/MongoDB CDC message
            canal_obj = None
            if isinstance(raw_val, str):
                try:
                    canal_obj = json.loads(raw_val)
                except Exception:
                    canal_obj = json.loads(base64.b64decode(raw_val).decode("utf-8"))
            else:
                canal_obj = raw_val

            canal_type = canal_obj.get("type")
            # print(f"[TRADE_RISK_V2_FC] Canal Event Type: {canal_type}")

            # V2 triggers on INSERT only — same as V1 philosophy
            if canal_type != "INSERT":
                # print(f"[TRADE_RISK_V2_FC] Skipping event type: {canal_type}")
                return f"SKIPPED_{canal_type}"

            data_row = canal_obj.get("data", [{}])[0]

            # V2 uses MongoDB field names: userId and _id
            user_code    = data_row.get("userId") or data_row.get("user_code") or data_row.get("userCode")
            txn_id_input = data_row.get("_id")    or data_row.get("id") or data_row.get("code")

            # Block MM_API orders at FC level as well (defense in depth)
            source = data_row.get("source", "")
            if source == "MM_API":
                # print(f"[TRADE_RISK_V2_FC] Skipping MM_API order, txn={txn_id_input}")
                return "SKIPPED_MM_API"

            print(f"[TRADE_RISK_V2_FC] Parsed Kafka: user={user_code}, txn={txn_id_input}, source={source}")

        # ---- HTTP API Trigger ----
        else:
            is_api_request = True
            body_str = envelope.get("body", "")
            if envelope.get("isBase64Encoded"):
                body_str = base64.b64decode(body_str).decode("utf-8")
            try:
                payload = json.loads(body_str)
            except Exception:
                payload = json.loads(event_str) if isinstance(envelope, str) else envelope

            user_code    = payload.get("user_code") or payload.get("userId")
            txn_id_input = payload.get("txn_id")    or payload.get("_id") or payload.get("id")

            # print(f"[TRADE_RISK_V2_FC] Parsed HTTP API: user={user_code}, txn={txn_id_input}")

        if not user_code or not txn_id_input:
            return _make_response(400, {"error": "Missing user_code or txn_id"})

    except Exception as exc:
        # print(f"[TRADE_RISK_V2_FC] FATAL PARSE ERROR: {exc}")
        return _make_response(400, {"error": f"Parse Failed: {str(exc)}"})

    # ==========================
    # 2. Fetch V2 Features (breaker-aware)
    # Step 3/4: OPEN -> fast-path via FAST_PATH_TRIGGER; CLOSED -> standard DB path.
    # Out-of-band probe runs inside execute_with_breaker when OPEN and cooldown elapsed.
    # ==========================
    retries = 10 if is_api_request else 40
    features = _wait_for_features_with_breaker(
        user_code, txn_id_input, max_retries=retries, delay=0.25
    )

    if features is circuit_breaker.FAST_PATH_TRIGGER:
        print("[TRADE_RISK_V2_FC] Breaker OPEN — skipping AST rules, routing to degraded fast-path.")
        return _handle_degraded_fast_path(user_code, txn_id_input, None, is_api_request)

    if not features:
        print(f"[TRADE_RISK_V2_FC] Features not ready: user={user_code}, txn={txn_id_input}")
        return _make_response(
            202 if is_api_request else 200,
            {"decision": "PENDING", "message": "Features not ready. Flink may still be processing."}
        )

    # print(f"[TRADE_RISK_V2_FC] Features retrieved: {json.dumps(features, default=str)[:800]}")

    # Ensure identity fields are in features dict for rule evaluation
    features["user_code"] = str(user_code)
    features["txn_id"]    = str(txn_id_input)

    # ==========================
    # 3. Evaluate Rules (AST — breaker-wrapped DB load)
    # ==========================
    rules = circuit_breaker.run_with_breaker(core.load_trade_rules_strict)
    if rules is circuit_breaker.FAST_PATH_TRIGGER:
        print("[TRADE_RISK_V2_FC] Rules load tripped breaker — routing to degraded fast-path.")
        return _handle_degraded_fast_path(user_code, txn_id_input, features, is_api_request)

    # print(f"[TRADE_RISK_V2_FC] Loaded {len(rules) if rules else 0} active rules")

    rule_result = core.evaluate_trade_rules(features, rules)
    # print(f"[TRADE_RISK_V2_FC] Rule Engine Result: {rule_result}")

    # ==========================
    # 4. Alert & Notify
    # ==========================
    if rule_result.get("triggered"):
        enforcement_actions = rule_result.get("enforcement_actions", [])
        alert_id = core.log_trade_alert(user_code, txn_id_input, rule_result, features, enforcement_actions)

        api_actions = [a for a in enforcement_actions if a != "LARK_ALERT"]
        has_automated_actions = bool(api_actions)

        api_success = False
        newly_applied = False
        api_skipped = False
        gateway_res = {}

        if has_automated_actions and alert_id:
            try:
                gateway_res = core.execute_gateway_actions(user_code, rule_result, alert_id)
                if gateway_res:
                    status = gateway_res.get("status")
                    api_success = status in (200, 201, 208)
                    newly_applied = gateway_res.get("newly_applied", False)
                    api_skipped = gateway_res.get("skipped", False)
            except Exception as e:
                print(f"[TRADE_RISK_V2_FC] Failed to execute gateway actions: {e}")

        response_data = {
            "user_code": str(user_code), "txn_id": str(txn_id_input), "decision": rule_result.get("decision"),
            "rule_hit": rule_result.get("rule_name"), "alert_type": rule_result.get("alert_type"),
            "narrative": rule_result.get("narrative"), "root_user_code": features.get("root_user_code", "N/A"),
            "inviter_user_code": features.get("inviter_user_code", "N/A"), "enforcement_actions": enforcement_actions
        }

        # Alex's Target State Logic
        must_alert_now = False
        use_debounce = False

        decision = rule_result.get("decision", "")
        already_active = gateway_res.get("already_active", False)
        is_whitelist = decision.strip() in ("Whitelist / Pass", "PASS")

        if not is_whitelist:
            if has_automated_actions:
                if not api_success and not is_shadow:
                    must_alert_now = True # Rule 3: Always alert on API failure
                elif newly_applied:
                    must_alert_now = True # Rule 2: Always alert when NEW restriction is applied
                elif already_active:
                    use_debounce = True            # already restricted → debounce (and suppress)
                else:
                    use_debounce = True   # Rule 4: Action was successful but skipped/duplicate, use 30m debounce
            else:
                use_debounce = True       # Rule 4: Lark-only rule, use 30m debounce

        if must_alert_now:
            core.send_lark_notification(response_data, features)
            core.update_lark_debounce_timestamp(str(user_code), str(rule_result.get("rule_id", "0")))
        elif use_debounce:
            suppress = core.should_suppress_lark(str(user_code), str(rule_result.get("rule_id", "0")))
            if suppress:
                print("[TRADE_RISK_V2_FC] Lark alert suppressed by OTS debounce cache.")
            else:
                core.send_lark_notification(response_data, features)

        elapsed = time.perf_counter() - start_ts
        return _make_response(200, response_data)

    # No rules triggered
    elapsed = time.perf_counter() - start_ts
    # print(f"[TRADE_RISK_V2_FC] PASS — no rules triggered. Elapsed: {elapsed:.3f}s")
    return _make_response(200, {
        "user_code": str(user_code),
        "txn_id":    str(txn_id_input),
        "decision":  "PASS"
    })