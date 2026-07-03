import json
import time
import hmac
import hashlib
import psycopg2
from psycopg2 import extensions as psyext
import urllib.request
import urllib.error
import ast
import uuid
from functools import lru_cache

import config as cfg

print("[TRADE_RISK_V2_FC] Loading core.py")

_RULES_CACHE     = None
_LAST_CACHE_TIME = 0
_DB_CONN         = None

# ==========================
# DB CONNECTION MANAGEMENT
# ==========================
def get_db_conn():
    global _DB_CONN
    if _DB_CONN is not None:
        try:
            if _DB_CONN.closed == 0:
                status = _DB_CONN.get_transaction_status()
                if status == psyext.TRANSACTION_STATUS_INERROR:
                    _DB_CONN.rollback()
                    return _DB_CONN
                if status == psyext.TRANSACTION_STATUS_UNKNOWN:
                    try:
                        _DB_CONN.close()
                    except Exception:
                        pass
                    _DB_CONN = None
                else:
                    return _DB_CONN
            else:
                _DB_CONN = None
        except Exception:
            try:
                _DB_CONN.close()
            except Exception:
                pass
            _DB_CONN = None

    _DB_CONN = psycopg2.connect(
        host=cfg.DB_HOST, port=cfg.DB_PORT, database=cfg.DB_NAME,
        user=cfg.DB_USER, password=cfg.DB_PASS, connect_timeout=3,
    )
    return _DB_CONN


def dict_factory(cursor, row):
    return {col.name: row[idx] for idx, col in enumerate(cursor.description)}


def ping_db():
    """Lightweight health probe for circuit-breaker out-of-band recovery."""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()


# ==========================
# FEATURE FETCHING
# V2: Queries risk_trade_features_v2
# V2: user_code and txn_id are BIGINT-compatible TEXT
# ==========================
def fetch_trade_features(user_code, txn_id):
    try:
        return fetch_trade_features_strict(user_code, txn_id)
    except Exception as exc:
        print(f"[TRADE_RISK_V2_FC] Error fetching features: {exc}")
        return None


def fetch_trade_features_strict(user_code, txn_id):
    """Fetch features; propagates DB connection/timeout errors for circuit breaker."""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM rt.risk_trade_features_v2 WHERE user_code = %s AND txn_id = %s",
        (str(user_code), str(txn_id)),
    )
    row = cur.fetchone()
    result = dict_factory(cur, row) if row else None
    cur.close()
    # print(
    #     f"[TRADE_RISK_V2_FC] DB fetch risk_trade_features_v2 -> "
    #     f"{'FOUND' if result else 'NOT FOUND'}"
    # )
    return result


def wait_for_trade_features(user_code, txn_id, max_retries=15, delay=0.25):
    """
    Retry loop to wait for Flink to populate the feature row.
    Flink has a small propagation delay after Kafka event arrives.
    """
    for attempt in range(max_retries):
        features = fetch_trade_features(user_code, txn_id)
        if features:
            return features
        print(f"[TRADE_RISK_V2_FC] Feature not ready, attempt {attempt + 1}/{max_retries}")
        time.sleep(delay)
    return None


# ==========================
# RULES — Load from V2 Table
# ==========================
def load_trade_rules():
    global _RULES_CACHE, _LAST_CACHE_TIME
    if _RULES_CACHE is not None and (time.time() - _LAST_CACHE_TIME < cfg.RULE_CACHE_TTL):
        return _RULES_CACHE

    try:
        rules = load_trade_rules_strict()
        _RULES_CACHE = rules
        _LAST_CACHE_TIME = time.time()
        return rules
    except Exception as exc:
        # print(f"[TRADE_RISK_V2_FC] Error loading rules: {exc}")
        return _RULES_CACHE if _RULES_CACHE else []


def load_trade_rules_strict():
    """Load active rules from DB; propagates connection errors for circuit breaker."""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM rt.risk_trade_rules_v2 WHERE status = 'ACTIVE' ORDER BY priority ASC"
    )
    rows = cur.fetchall()
    rules = [dict_factory(cur, row) for row in rows] if rows else []
    cur.close()
    # print(f"[TRADE_RISK_V2_FC] Rules loaded from DB: {len(rules)} active rules")
    return rules


# ==========================
# ALERT LOGGING — V2 Table
# ==========================
def log_trade_alert(user_code, txn_id, result, features, enforcement_actions=None):
    try:
        conn = get_db_conn()
        cur  = conn.cursor()

        alert_id     = str(uuid.uuid4())
        action_taken = result.get("decision", "HOLD")

        # Include correlated UIDs only for HOLD ALL decisions
        correlated_uids = (
            features.get("correlated_account_uids", "")
            if "ALL" in action_taken
            else ""
        )

        # V2: inserts into risk_trade_alerts_v2
        insert_sql = """
            INSERT INTO rt.risk_trade_alerts_v2
            (alert_id, user_code, txn_id, correlated_uids, rule_id, rule_name,
             action_taken, feature_snapshot, status, enforcement_actions_taken, detected_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        cur.execute(
            insert_sql,
            (
                alert_id,
                str(user_code),
                str(txn_id),
                correlated_uids,
                result.get("rule_id"),
                result.get("rule_name"),
                action_taken,
                json.dumps(features, default=str),
                "ACTIVE",
                json.dumps(enforcement_actions) if enforcement_actions else '[]',
            )
        )
        conn.commit()
        cur.close()
        # print(f"[TRADE_RISK_V2_FC] Alert logged: rule={result.get('rule_name')}, action={action_taken}")
        return alert_id
    except Exception as exc:
        # print(f"[TRADE_RISK_V2_FC] Error logging alert: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None


# ==========================
# AST RULE EVALUATOR
# Identical safe sandbox as V1 — works for all V2 rules
# ==========================
def _to_bool(v):
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "yes", "y", "1"):  return True
        if s in ("false", "f", "no", "n", "0", ""): return False
    return False


def _to_number(v):
    if isinstance(v, (int, float)): return float(v)
    if isinstance(v, str):
        try: return float(v.strip())
        except: return None
    return None


def _normalize_feature_value(k, v):
    """
    Normalizes feature values for safe AST evaluation.
    V2 additions:
    - consistent_ prefix for boolean funding/position features
    - current_ prefix for trigger context fields
    - wallet_ prefix for balance fields
    - volatility suffix for numeric fields
    """
    if v is None:
        return 0.0
    key = (k or "").lower()

    # Boolean feature patterns
    bool_patterns = ("is_", "has_", "user_whitelisted", "user_blacklisted",
                     "user_greylisted", "consistent_")
    if any(key.startswith(p) or key == p for p in bool_patterns):
        return _to_bool(v)

    # Numeric feature patterns
    numeric_patterns = ("ratio", "rate", "count", "amount", "volume",
                        "profit", "pnl", "fee", "rebate", "benefit",
                        "days", "seconds", "ms", "size", "balance",
                        "inflow", "income", "volatility")
    if any(p in key for p in numeric_patterns):
        return _to_number(v) or 0.0

    # Current order context fields (always numeric)
    if key.startswith("current_"):
        return _to_number(v) or 0.0

    return v


_ALLOWED_NODES = (
    ast.Expression, ast.BoolOp, ast.UnaryOp, ast.BinOp, ast.Compare,
    ast.Name, ast.Load, ast.Constant,
    ast.And, ast.Or, ast.Not,
    ast.UAdd, ast.USub,
    ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Mod,
    ast.Eq, ast.NotEq, ast.Gt, ast.GtE, ast.Lt, ast.LtE,
)


def _validate_ast(node: ast.AST):
    for n in ast.walk(node):
        if not isinstance(n, _ALLOWED_NODES):
            raise ValueError(f"Disallowed AST node: {type(n).__name__}")


@lru_cache(maxsize=2048)
def _compile_rule_expr(expr: str):
    expr = (expr or "").replace("\n", " ").replace("\r", " ").strip()
    if not expr:
        return None
    tree = ast.parse(expr, mode="eval")
    _validate_ast(tree)
    return tree


def _eval_ast(node: ast.AST, ctx: dict):
    if isinstance(node, ast.Expression):  return _eval_ast(node.body, ctx)
    if isinstance(node, ast.Constant):    return node.value
    if isinstance(node, ast.Name):        return ctx.get(node.id, 0)

    if isinstance(node, ast.UnaryOp):
        val = _eval_ast(node.operand, ctx)
        if isinstance(node.op, ast.Not):  return not bool(val)
        if isinstance(node.op, ast.USub): return -float(val)

    if isinstance(node, ast.BoolOp):
        if isinstance(node.op, ast.And):
            return all(bool(_eval_ast(v, ctx)) for v in node.values)
        if isinstance(node.op, ast.Or):
            return any(bool(_eval_ast(v, ctx)) for v in node.values)

    if isinstance(node, ast.BinOp):
        l = _eval_ast(node.left, ctx)
        r = _eval_ast(node.right, ctx)
        if isinstance(node.op, ast.Add):  return l + r
        if isinstance(node.op, ast.Sub):  return l - r
        if isinstance(node.op, ast.Mult): return l * r
        if isinstance(node.op, ast.Div):  return l / r if r != 0 else 0
        if isinstance(node.op, ast.Mod):  return l % r if r != 0 else 0

    if isinstance(node, ast.Compare):
        l = _eval_ast(node.left, ctx)
        for op, comp in zip(node.ops, node.comparators):
            r = _eval_ast(comp, ctx)
            if   isinstance(op, ast.Eq):    res = (l == r)
            elif isinstance(op, ast.NotEq): res = (l != r)
            elif isinstance(op, ast.Gt):    res = (l >  r)
            elif isinstance(op, ast.GtE):   res = (l >= r)
            elif isinstance(op, ast.Lt):    res = (l <  r)
            elif isinstance(op, ast.LtE):   res = (l <= r)
            else:                           res = False
            if not res: return False
            l = r
        return True

    return False


def evaluate_trade_rules(features, rules):
    """
    Evaluates all active rules in priority order.
    Returns first triggered rule result or triggered=False.
    """
    safe_locals = {
        k: _normalize_feature_value(k, v)
        for k, v in (features or {}).items()
    }

    for rule in rules or []:
        try:
            expr = rule.get("logic_expression", "")
            tree = _compile_rule_expr(expr)
            if tree and bool(_eval_ast(tree, safe_locals)):
                # print(f"[TRADE_RISK_V2_FC] Rule triggered: #{rule.get('rule_id')} — {rule.get('rule_name')}")
                rule_actions = rule.get("enforcement_actions")
                if isinstance(rule_actions, str):
                    try:
                        rule_actions = json.loads(rule_actions)
                    except:
                        rule_actions = []
                elif not isinstance(rule_actions, list):
                    rule_actions = []
                return {
                    "triggered":  True,
                    "decision":   (rule.get("action") or "Human Monitoring").strip(),
                    "rule_id":    rule.get("rule_id"),
                    "rule_name":  rule.get("rule_name"),
                    "alert_type": rule.get("alert_type", "Unknown Type"),
                    "narrative":  f"[Rule #{rule.get('rule_id')}] {rule.get('narrative')}",
                    "enforcement_actions": rule_actions,
                }
        except Exception as exc:
            # print(f"[TRADE_RISK_V2_FC] AST eval failed Rule #{rule.get('rule_id')} "
            #       f"({rule.get('rule_name')}): {exc}")
            continue

    return {"triggered": False}


# ==========================
# LARK NOTIFICATION — V2 Enhanced Card
# Adds V2-specific fields: cancel rate, margin utilization,
# opposite party concentration, funding income
# ==========================
def send_lark_notification(data, features):
    if not cfg.LARK_WEBHOOK_URL:
        return
    try:
        decision   = data.get("decision", "")
        rule_name  = data.get("rule_hit", "Unknown Rule")
        alert_type = data.get("alert_type", "Alert")

        # Dynamic header colors based on action type
        color_map = {
            "Automated Actions": "red",
            "Human Monitoring": "orange",
            "Whitelist / Pass": "green"
        }
        header_color = color_map.get(decision, "blue")

        # Format enforcement actions for display
        actions_list = data.get("enforcement_actions") or []
        actions_str = ", ".join(actions_list) if actions_list else "None"

        # Show correlated UIDs only for HOLD ALL decisions
        correlated = (
            features.get("correlated_account_uids", "None")
            if "ALL" in decision
            else "N/A"
        )

        # Core metrics
        vol              = float(features.get("trading_volume", 0.0) or 0.0)
        pnl              = float(features.get("net_pnl", 0.0) or 0.0)
        fee              = float(features.get("trading_fees_paid", 0.0) or 0.0)
        offset           = float(features.get("offset_ratio", 0.0) or 0.0)
        rebate           = float(features.get("total_rebate", 0.0) or 0.0)
        net_benefit      = float(features.get("net_benefit", 0.0) or 0.0)

        # V2 new metrics
        cancel_rate      = float(features.get("order_cancel_rate", 0.0) or 0.0)
        margin_util      = float(features.get("margin_utilization_ratio", 0.0) or 0.0)
        opp_conc         = float(features.get("opposite_party_concentration", 0.0) or 0.0)
        funding_income   = float(features.get("funding_fee_income", 0.0) or 0.0)
        cashback         = float(features.get("cashback_received", 0.0) or 0.0)
        cancel_ms        = float(features.get("avg_order_lifetime_ms", 0.0) or 0.0)
        current_symbol   = features.get("current_symbol_id", "N/A")
        current_leverage = features.get("current_leverage", "N/A")
        current_side     = {1: "Buy", 2: "Sell"}.get(features.get("current_order_side"), "N/A")
        wallet_bal       = float(features.get("wallet_balance", 0.0) or 0.0)

        card_content = {
            "msg_type": "interactive",
            "card": {
                "config": {"wide_screen_mode": True},
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": f"🚨 TRADE RISK V2: {alert_type} — {rule_name}"
                    },
                    "template": header_color
                },
                "elements": [
                    # --- User Identity ---
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**User:**\n{data.get('user_code')}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Root User:**\n{data.get('root_user_code', 'N/A')}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Inviter:**\n{data.get('inviter_user_code', 'N/A')}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Txn ID:**\n{data.get('txn_id')}"}},
                        ]
                    },
                    {"tag": "hr"},
                    # --- Action & Enforcement Details ---
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Trigger Type:**\n{decision}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**System Actions:**\n{actions_str}"}},
                        ]
                    },
                    {"tag": "hr"},
                    # --- Trade Context ---
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Symbol ID:**\n{current_symbol}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Side:**\n{current_side}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Leverage:**\n{current_leverage}x"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Wallet Bal:**\n{wallet_bal:,.2f}"}},
                        ]
                    },
                    {"tag": "hr"},
                    # --- Core Trading Metrics ---
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Vol (USDT):**\n{vol:,.2f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Net P&L:**\n{pnl:,.2f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Fee Paid:**\n{fee:,.2f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Net Benefit:**\n{net_benefit:,.2f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Offset Ratio:**\n{offset:.4f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Total Rebate:**\n{rebate:,.2f}"}},
                        ]
                    },
                    {"tag": "hr"},
                    # --- V2 New Risk Signals ---
                    {
                        "tag": "div",
                        "fields": [
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Cancel Rate:**\n{cancel_rate:.2%}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Avg Cancel ms:**\n{cancel_ms:,.0f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Margin Util:**\n{margin_util:.2%}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Opp Party Conc:**\n{opp_conc:.2%}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Funding Income:**\n{funding_income:,.2f}"}},
                            {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Cashback:**\n{cashback:,.2f}"}},
                        ]
                    },
                    {"tag": "hr"},
                    # --- Correlated Accounts ---
                    {
                        "tag": "div",
                        "text": {"tag": "lark_md", "content": f"**Correlated UIDs:**\n{correlated}"}
                    },
                    {"tag": "hr"},
                    # --- Rule Details ---
                    {
                        "tag": "div",
                        "text": {
                            "tag": "lark_md",
                            "content": f"**Triggered Rule:**\n{rule_name}\n\n**Reasoning:**\n{data.get('narrative', '')}"
                        }
                    },
                ]
            }
        }

        req = urllib.request.Request(
            cfg.LARK_WEBHOOK_URL,
            data=json.dumps(card_content).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=3)
        # print(f"[TRADE_RISK_V2_FC] Lark notification sent successfully")

    except Exception as e:
        print(f"[TRADE_RISK_V2_FC] Lark notification error: {e}")


# ==========================
# GATEWAY CLIENT — Automated Enforcement + Audit Log
# ==========================
def execute_gateway_actions(user_code, rule_result, alert_id):
    import uuid # Ensure uuid is imported if not already at the top

    actions = rule_result.get("enforcement_actions", [])
    api_actions = [a for a in actions if a != "LARK_ALERT"]

    if not api_actions or not alert_id:
        return None

    payload = {
        "uid": int(user_code) if str(user_code).isdigit() else user_code,
        "source_order_id": f"phalanx-lock-{user_code}-{rule_result.get('rule_id', '0')}",
        "alert_type": rule_result.get("alert_type", "Unknown"),
        "actions": [{"action": act, "reason": rule_result.get("rule_name", "Risk Engine Trigger"), "expire_seconds": 0} for act in api_actions]
    }

    # CRITICAL: Must be deterministic for HMAC signature matching
    payload_bytes = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode("utf-8")

    signature = hmac.new(
        key=getattr(cfg, 'RISK_GATEWAY_SECRET', '').encode("utf-8"),
        msg=payload_bytes,
        digestmod=hashlib.sha256
    ).hexdigest()

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": getattr(cfg, 'RISK_GATEWAY_API_KEY', ''),
        "X-Signature": signature,
        "Host": "prod-admin-in.onebullex.com"
    }

    is_shadow = not getattr(cfg, 'ENABLE_AUTOMATED_ACTIONS', False)
    http_status = None
    response_body = ""
    latency = 0

    if not is_shadow:
        start_t = time.perf_counter()
        try:
            req = urllib.request.Request(getattr(cfg, 'RISK_GATEWAY_URL', ''), data=payload_bytes, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=getattr(cfg, 'RISK_GATEWAY_TIMEOUT', 3)) as response:
                http_status = response.getcode()
                response_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            http_status = e.code
            response_body = e.read().decode("utf-8")
        except Exception as e:
            http_status = 500
            response_body = str(e)
        latency = int((time.perf_counter() - start_t) * 1000)

    # Write to Audit Log (Records true calls and shadow mode)
    try:
        conn = get_db_conn()
        cur = conn.cursor()
        audit_id = str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO rt.risk_trade_action_audit_log
            (audit_id, alert_id, user_code, rule_id, api_endpoint, payload_sent, hmac_signature, http_status_code, response_body, latency_ms, is_shadow_mode, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (audit_id, alert_id, str(user_code), rule_result.get("rule_id", 0), getattr(cfg, 'RISK_GATEWAY_URL', ''), json.dumps(payload), signature, http_status, response_body, latency, is_shadow)
        )
        conn.commit()
        cur.close()
    except Exception as exc:
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass

    newly_applied = False
    if not is_shadow and response_body:
        try:
            resp_json = json.loads(response_body)
            # Handle variations in Exchange API response shapes
            data_list = resp_json.get("results") or resp_json.get("data") or []
            if isinstance(data_list, list):
                for item in data_list:
                    res_val = str(item.get("Result") or item.get("result") or "").lower()
                    if res_val == "applied":
                        newly_applied = True
                        break
        except Exception:
            pass # If parsing fails, rely on standard alerting

    return {"status": http_status, "shadow_mode": is_shadow, "skipped": False, "newly_applied": newly_applied}


def should_suppress_lark(user_code, rule_id):
    """Strict 30-minute (1800s) rolling debounce using OTS."""
    try:
        import os, time
        from tablestore import OTSClient, Row, Condition, RowExistenceExpectation
        endpoint, instance_name = os.environ.get("OTS_ENDPOINT"), os.environ.get("OTS_INSTANCE")
        ak_id, ak_secret = os.environ.get("OTS_ACCESS_KEY_ID"), os.environ.get("OTS_ACCESS_KEY_SECRET")
        if not all([endpoint, instance_name, ak_id, ak_secret]): return False

        client = OTSClient(endpoint, ak_id, ak_secret, instance_name)
        table_name = "phalanx_alert_cache"
        primary_key = [("cache_key", f"TR_{user_code}_{rule_id}")]

        try: _, row, _ = client.get_row(table_name, primary_key, ["last_alert_ts"])
        except Exception: row = None

        current_ts = int(time.time())
        prev_ts = int(row.attribute_columns[0][1]) if row and row.attribute_columns else 0

        if (current_ts - prev_ts) < 1800:
            return True

        client.put_row(table_name, Row(primary_key, [("last_alert_ts", current_ts)]), Condition(RowExistenceExpectation.IGNORE))
        return False
    except Exception as e:
        print(f"[TRADE_RISK_V2_FC] OTS Debounce error: {e}")
        return False # Fail open


def update_lark_debounce_timestamp(user_code, rule_id):
    """Unconditionally updates the last_alert_ts in OTS after a forced alert."""
    try:
        import os, time
        from tablestore import OTSClient, Row, Condition, RowExistenceExpectation
        endpoint, instance_name = os.environ.get("OTS_ENDPOINT"), os.environ.get("OTS_INSTANCE")
        ak_id, ak_secret = os.environ.get("OTS_ACCESS_KEY_ID"), os.environ.get("OTS_ACCESS_KEY_SECRET")
        if not all([endpoint, instance_name, ak_id, ak_secret]): return

        client = OTSClient(endpoint, ak_id, ak_secret, instance_name)
        table_name = "phalanx_alert_cache"
        primary_key = [("cache_key", f"TR_{user_code}_{rule_id}")]

        client.put_row(
            table_name,
            Row(primary_key, [("last_alert_ts", int(time.time()))]),
            Condition(RowExistenceExpectation.IGNORE)
        )
    except Exception as e:
        print(f"[TRADE_RISK_V2_FC] Failed to update OTS timestamp: {e}")