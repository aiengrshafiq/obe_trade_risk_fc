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
import math
from functools import lru_cache
from tablestore import OTSClient, RowExistenceExpectation, Condition, SingleColumnCondition, ComparatorType, Row, LogicalOperator, CompositeColumnCondition
import tablestore
import os
import re
#from tablestore import OTSClient, Row, Condition, RowExistenceExpectation

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
        """
        SELECT r.*, t.template_text 
        FROM rt.risk_trade_rules_v2 r 
        LEFT JOIN rt.risk_alert_templates t ON r.template_id = t.template_id 
        WHERE r.status = 'ACTIVE' ORDER BY r.priority ASC
        """
    )
    rows = cur.fetchall()
    rules = [dict_factory(cur, row) for row in rows] if rows else []
    cur.close()
    # print(f"[TRADE_RISK_V2_FC] Rules loaded from DB: {len(rules)} active rules")
    return rules


# ==========================
# ALERT LOGGING — V2 Table
# ==========================
# def log_trade_alert(user_code, txn_id, result, features, enforcement_actions=None):
#     try:
#         conn = get_db_conn()
#         cur  = conn.cursor()

#         alert_id     = str(uuid.uuid4())
#         action_taken = result.get("decision", "HOLD")

#         # Include correlated UIDs only for HOLD ALL decisions
#         correlated_uids = (
#             features.get("correlated_account_uids", "")
#             if "ALL" in action_taken
#             else ""
#         )

#         # V2: inserts into risk_trade_alerts_v2
#         insert_sql = """
#             INSERT INTO rt.risk_trade_alerts_v2
#             (alert_id, user_code, txn_id, correlated_uids, rule_id, rule_name,
#              action_taken, feature_snapshot, status, enforcement_actions_taken, detected_at)
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
#         """
#         cur.execute(
#             insert_sql,
#             (
#                 alert_id,
#                 str(user_code),
#                 str(txn_id),
#                 correlated_uids,
#                 result.get("rule_id"),
#                 result.get("rule_name"),
#                 action_taken,
#                 json.dumps(features, default=str),
#                 "ACTIVE",
#                 json.dumps(enforcement_actions) if enforcement_actions else '[]',
#             )
#         )
#         conn.commit()
#         cur.close()
#         # print(f"[TRADE_RISK_V2_FC] Alert logged: rule={result.get('rule_name')}, action={action_taken}")
#         return alert_id
#     except Exception as exc:
#         # print(f"[TRADE_RISK_V2_FC] Error logging alert: {exc}")
#         try:
#             conn.rollback()
#         except Exception:
#             pass
#         return None

def log_trade_alert(user_code, txn_id, result, features, enforcement_actions=None):
    try:
        conn = get_db_conn()
        cur  = conn.cursor()

        is_tiered = result.get("is_tiered", False)
        current_tier = result.get("current_tier", 0.0)
        rule_id = result.get("rule_id")

        if is_tiered:
            # Deterministic UUID5 for Idempotent Insert (Claude's mandate)
            namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
            alert_id = str(uuid.uuid5(namespace, f"{user_code}_{rule_id}_{current_tier}"))
        else:
            alert_id = str(uuid.uuid4())
            
        action_taken = result.get("decision", "HOLD")

        correlated_uids = (
            features.get("correlated_account_uids", "")
            if "ALL" in action_taken else ""
        )

        # UPSERT logic (ON CONFLICT DO NOTHING requires a UNIQUE constraint in Postgres, 
        # Hologres supports this via Primary Key on (alert_id))
        insert_sql = """
            INSERT INTO rt.risk_trade_alerts_v2
            (alert_id, user_code, txn_id, correlated_uids, rule_id, rule_name,
             action_taken, feature_snapshot, status, enforcement_actions_taken, detected_at, tier_triggered, trigger_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, 1)
            ON CONFLICT (user_code, alert_id) DO NOTHING
        """
        cur.execute(
            insert_sql,
            (
                alert_id, str(user_code), str(txn_id), correlated_uids, rule_id, result.get("rule_name"),
                action_taken, json.dumps(features, default=str), "ACTIVE",
                json.dumps(enforcement_actions) if enforcement_actions else '[]',
                current_tier if is_tiered else None
            )
        )
        conn.commit()
        cur.close()
        
        # FINAL STEP: Now that DB write is safe, advance the OTS High-Water Mark (Scenario B Completion)
        if is_tiered:
            _advance_ots_high_water_mark(user_code, rule_id, current_tier)

        return alert_id
    except Exception as exc:
        # print(f"[TRADE_RISK_V2_FC] Error logging alert: {exc}")
        print(f"[URGENT DEBUG] Error logging alert to Database: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None

def _advance_ots_high_water_mark(user_code, rule_id, new_tier):
    import os, time
    import tablestore
 
    try:
        endpoint = os.environ.get("OTS_ENDPOINT")
        instance_name = os.environ.get("OTS_INSTANCE")
        ak_id = os.environ.get("OTS_ACCESS_KEY_ID")
        ak_secret = os.environ.get("OTS_ACCESS_KEY_SECRET")
        if not all([endpoint, instance_name, ak_id, ak_secret]):
            return
            
        client = tablestore.OTSClient(endpoint, ak_id, ak_secret, instance_name)
        
        table_name = "phalanx_alert_cache"
        primary_key = [("cache_key", f"TIER_{user_code}_{rule_id}")]
        
        update_attrs = {
            'PUT': [
                ("high_water_tier", float(new_tier)),
                ("trigger_count", 1),
                ("last_evidence_ts", int(time.time()))
            ]
        }
        
        row = tablestore.Row(primary_key, update_attrs)
        condition = tablestore.Condition(tablestore.RowExistenceExpectation.IGNORE)
        
        client.update_row(table_name, row, condition)
        
    except Exception as e:
        print(f"[URGENT DEBUG] Failed to advance HWM in OTS: {repr(e)}")


# def _advance_ots_high_water_mark(user_code, rule_id, current_tier):
#     """Called only AFTER Hologres durably stores the alert."""
#     import os, time
#     from tablestore import OTSClient, Row, Condition, RowExistenceExpectation, SingleColumnCondition, ComparatorType, UpdateRowItem
    
#     endpoint, instance_name = os.environ.get("OTS_ENDPOINT"), os.environ.get("OTS_INSTANCE")
#     ak_id, ak_secret = os.environ.get("OTS_ACCESS_KEY_ID"), os.environ.get("OTS_ACCESS_KEY_SECRET")
#     if not all([endpoint, instance_name, ak_id, ak_secret]): return
    
#     client = OTSClient(endpoint, ak_id, ak_secret, instance_name)
#     table_name = "phalanx_alert_cache"
#     primary_key = [("cache_key", f"TIER_{user_code}_{rule_id}")]
    
#     try:
#         # We don't know the exact old HWM here without a read, but because we only get here 
#         # if evaluate_trade_rules passed Scenario B, we know we are advancing.
#         # We simply CAS ensure high_water_tier < current_tier
#         cond = Condition(RowExistenceExpectation.EXPECT_EXIST, SingleColumnCondition("high_water_tier", current_tier, ComparatorType.LESS_THAN))
#         update_of = UpdateRowItem()
#         update_of.put([("high_water_tier", current_tier), ("last_evidence_ts", int(time.time()))])
#         # Note: We reset count to 1 on a tier advance, but only if the tier is actually less.
#         update_of.put([("trigger_count", 1)])
        
#         client.update_row(table_name, Row(primary_key), cond, update_of)
#     except Exception as e:
#         print(f"[TRADE_RISK_V2_FC] Failed to advance HWM in OTS: {e}")
#         print(f"[URGENT DEBUG] Failed to advance HWM in OTS: {repr(e)}")
#         pass


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

def _process_tier_logic(user_code, rule_id, target_feature, step_size, feature_val, features):
    """
    Implements the Claude-reviewed state machine for Tiered Triggers.
    Returns (should_alert, current_tier, hwm_tier).
    """
    import os
    from tablestore import OTSClient
    
    endpoint, instance_name = os.environ.get("OTS_ENDPOINT"), os.environ.get("OTS_INSTANCE")
    ak_id, ak_secret = os.environ.get("OTS_ACCESS_KEY_ID"), os.environ.get("OTS_ACCESS_KEY_SECRET")
    
    if not all([endpoint, instance_name, ak_id, ak_secret]):
        print("[TRADE_RISK_V2_FC] Missing OTS creds for Tiering. Failing Open (Alerting).")
        return True, 0, 0

    client = OTSClient(endpoint, ak_id, ak_secret, instance_name)
    table_name = "phalanx_alert_cache" # Reuse existing table, different key prefix
    primary_key = [("cache_key", f"TIER_{user_code}_{rule_id}")]
    
    # Calculate the absolute tier (e.g., 1020 / 500 = 2.0 * 500 = 1000)
    current_tier = float(math.floor(float(feature_val) / float(step_size)) * float(step_size))
    now_ts = int(time.time())

    try:
        _, row, _ = client.get_row(table_name, primary_key, ["high_water_tier", "trigger_count", "last_evidence_ts"])
    except Exception as e:
        print(f"[TRADE_RISK_V2_FC] OTS get_row failed: {e}. Failing Open.")
        print(f"[URGENT DEBUG] Scenario C OTS update failed: {repr(e)}")
        return True, current_tier, 0

    # ==========================================
    # SCENARIO A: Cold Start (Silent Seeding)
    # ==========================================
    if row is None:
        try:
            client.put_row(
                table_name,
                Row(primary_key, [
                    ("high_water_tier", current_tier),
                    ("trigger_count", 1),
                    ("last_evidence_ts", now_ts)
                ]),
                Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)
            )
            print(f"[TRADE_RISK_V2_FC] Seeded Tier {current_tier} for {user_code}_{rule_id}. Suppressing.")
        except Exception:
            print(f"[TRADE_RISK_V2_FC] If EXPECT_NOT_EXIST fails, someone beat us to seeding. Fall through to read again next time.")
            # If EXPECT_NOT_EXIST fails, someone beat us to seeding. Fall through to read again next time.
            pass
        return False, current_tier, current_tier

    # Extract existing state
    attrs = {k: v for k, v, _ in row.attribute_columns} if row.attribute_columns else {}
    hwm_tier = float(attrs.get("high_water_tier", 0.0))
    last_ts = int(attrs.get("last_evidence_ts", 0))

    # ==========================================
    # SCENARIO B: Crossing a New Tier
    # ==========================================
    if current_tier > hwm_tier:
        # 1. We return True to let the FC know it MUST alert. 
        # The FC will handle the idempotent DB insert FIRST, then call a callback to update OTS.
        return True, current_tier, hwm_tier

    # ==========================================
    # SCENARIO C: Dip / Same Tier (Repeated Trigger)
    # ==========================================
    else:
        # Atomic Increment (Python SDK Dict syntax)
        try:
            import tablestore
            update_attrs = {
                'INCREMENT': [("trigger_count", 1)]
            }
            
            should_flush = (now_ts - last_ts) >= 60
            if should_flush:
                update_attrs['PUT'] = [("last_evidence_ts", now_ts)]
                cond = tablestore.Condition(
                    tablestore.RowExistenceExpectation.EXPECT_EXIST, 
                    tablestore.SingleColumnCondition("last_evidence_ts", last_ts, tablestore.ComparatorType.EQUAL)
                )
            else:
                cond = tablestore.Condition(tablestore.RowExistenceExpectation.EXPECT_EXIST)
                
            row_to_update = tablestore.Row(primary_key, update_attrs)
            
            # Python SDK update_row (No return_type because RT_AFTER_MODIFY is not supported)
            client.update_row(table_name, row_to_update, cond)
            
            if should_flush:
                # Deterministic UUID5 based on the High Water Mark tier
                namespace = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')
                alert_id = str(uuid.uuid5(namespace, f"{user_code}_{rule_id}_{hwm_tier}"))
                
                conn = get_db_conn()
                cur = conn.cursor()
                
                # RESTORED: Updating the feature_snapshot so the Dashboard shows the latest trade details.
                # NATIVE INCREMENT: Using SQL to increment the count since OTS Python SDK can't return it.
                # cur.execute(
                #     """
                #     UPDATE rt.risk_trade_alerts_v2 
                #     SET trigger_count = trigger_count + 1, 
                #         feature_snapshot = %s, 
                #         updated_at = NOW() 
                #     WHERE alert_id = %s
                #     """,
                #     (json.dumps(features, default=str), alert_id)
                # )
                cur.execute(
                    """
                    UPDATE rt.risk_trade_alerts_v2 
                    SET trigger_count = trigger_count + 1, 
                        feature_snapshot = %s 
                    WHERE alert_id = %s
                    """,
                    (json.dumps(features, default=str), alert_id)
                )
                conn.commit()
                cur.close()

        except Exception as e:
            print(f"[URGENT DEBUG] Scenario C OTS update failed: {repr(e)}")
            
        return False, current_tier, hwm_tier

def evaluate_trade_rules(features, rules):
    """
    Evaluates all active rules. Supports standard and Tiered triggers.
    """
    safe_locals = {
        k: _normalize_feature_value(k, v)
        for k, v in (features or {}).items()
    }

    user_code = features.get("user_code")

    for rule in rules or []:
        try:
            expr = rule.get("logic_expression", "")
            tree = _compile_rule_expr(expr)
            
            if tree and bool(_eval_ast(tree, safe_locals)):
                
                # Check for Tiered Trigger Configuration
                target_feature = rule.get("tier_target_feature")
                step_size = _to_number(rule.get("tier_step_size"))
                
                rule_id = rule.get("rule_id")
                current_tier = 0
                
                if target_feature and step_size and step_size > 0:
                    feature_val = safe_locals.get(target_feature, 0.0)
                    should_alert, current_tier, _ = _process_tier_logic(user_code, rule_id, target_feature, step_size, feature_val, features)
                    
                    if not should_alert:
                        continue # Suppress alert, move to next rule
                
                rule_actions = rule.get("enforcement_actions")
                if isinstance(rule_actions, str):
                    try: rule_actions = json.loads(rule_actions)
                    except: rule_actions = []
                elif not isinstance(rule_actions, list):
                    rule_actions = []
                    
                return {
                    "triggered":  True,
                    "decision":   (rule.get("action") or "Human Monitoring").strip(),
                    "rule_id":    rule_id,
                    "rule_name":  rule.get("rule_name"),
                    "alert_type": rule.get("alert_type", "Unknown Type"),
                    "narrative":  f"[Rule #{rule_id}] {rule.get('narrative')}",
                    "enforcement_actions": rule_actions,
                    "is_tiered": bool(target_feature),
                    "current_tier": current_tier,
                    "alert_group_name": rule.get("alert_group_name"),
                    "template_text": rule.get("template_text")
                }
        except Exception as exc:
            print(f"[URGENT DEBUG] AST eval failed for Rule #{rule.get('rule_id')}: {exc}")
            continue

    return {"triggered": False}

# def evaluate_trade_rules(features, rules):
#     """
#     Evaluates all active rules in priority order.
#     Returns first triggered rule result or triggered=False.
#     """
#     safe_locals = {
#         k: _normalize_feature_value(k, v)
#         for k, v in (features or {}).items()
#     }

#     for rule in rules or []:
#         try:
#             expr = rule.get("logic_expression", "")
#             tree = _compile_rule_expr(expr)
#             if tree and bool(_eval_ast(tree, safe_locals)):
#                 # print(f"[TRADE_RISK_V2_FC] Rule triggered: #{rule.get('rule_id')} — {rule.get('rule_name')}")
#                 rule_actions = rule.get("enforcement_actions")
#                 if isinstance(rule_actions, str):
#                     try:
#                         rule_actions = json.loads(rule_actions)
#                     except:
#                         rule_actions = []
#                 elif not isinstance(rule_actions, list):
#                     rule_actions = []
#                 return {
#                     "triggered":  True,
#                     "decision":   (rule.get("action") or "Human Monitoring").strip(),
#                     "rule_id":    rule.get("rule_id"),
#                     "rule_name":  rule.get("rule_name"),
#                     "alert_type": rule.get("alert_type", "Unknown Type"),
#                     "narrative":  f"[Rule #{rule.get('rule_id')}] {rule.get('narrative')}",
#                     "enforcement_actions": rule_actions,
#                 }
#         except Exception as exc:
#             # print(f"[TRADE_RISK_V2_FC] AST eval failed Rule #{rule.get('rule_id')} "
#             #       f"({rule.get('rule_name')}): {exc}")
#             continue

#     return {"triggered": False}


# ==========================
# LARK NOTIFICATION — V2 Enhanced Card
# Adds V2-specific fields: cancel rate, margin utilization,
# opposite party concentration, funding income
# ==========================
def send_lark_notification(data, features, alert_id=None):
    try:
        decision   = data.get("decision", "")
        rule_name  = data.get("rule_hit", "Unknown Rule")
        alert_type = data.get("alert_type", "Alert")
        
        template_text = data.get("template_text")
        alert_group_name = data.get("alert_group_name")
        
        # 1. Resolve Webhook Routing
        webhook_url = cfg.ALERT_GROUPS.get(alert_group_name) or cfg.ALERT_GROUPS.get("DEFAULT")
        if not webhook_url:
            return

        # Dynamic header colors based on action type
        color_map = {
            "Automated Actions": "red",
            "Human Monitoring": "orange",
            "Whitelist / Pass": "green"
        }
        header_color = color_map.get(decision, "blue")
        
        final_rendered_msg = ""

        # ==========================================
        # 2A. NEW FLOW: Dynamic Markdown Template
        # ==========================================
        if template_text:
            def replacer(match):
                key = match.group(1)
                val = features.get(key)
                if val is None:
                    val = data.get(key, "N/A")
                return str(val)
                
            # Safely replace ${field_name} with snapshot values
            final_rendered_msg = re.sub(r'\$\{([^}]+)\}', replacer, template_text)
            
            card_content = {
                "msg_type": "interactive",
                "card": {
                    "config": {"wide_screen_mode": True},
                    "header": {
                        "title": {"tag": "plain_text", "content": f"🚨 TRADE RISK V2: {alert_type} — {rule_name}"},
                        "template": header_color
                    },
                    "elements": [
                        {"tag": "markdown", "content": final_rendered_msg}
                    ]
                }
            }

        # ==========================================
        # 2B. LEGACY FLOW: Default Hardcoded Card
        # ==========================================
        else:
            actions_list = data.get("enforcement_actions") or []
            actions_str = ", ".join(actions_list) if actions_list else "None"
            correlated = features.get("correlated_account_uids", "None") if "ALL" in decision else "N/A"
            
            vol = float(features.get("trading_volume", 0.0) or 0.0)
            pnl = float(features.get("net_pnl", 0.0) or 0.0)
            
            final_rendered_msg = f"Legacy fallback triggered for {rule_name}. Vol: {vol}, PnL: {pnl}"
            
            card_content = {
                "msg_type": "interactive",
                "card": {
                    "config": {"wide_screen_mode": True},
                    "header": {
                        "title": {"tag": "plain_text", "content": f"🚨 TRADE RISK V2: {alert_type} — {rule_name}"},
                        "template": header_color
                    },
                    "elements": [
                        {
                            "tag": "div",
                            "fields": [
                                {"is_short": True,  "text": {"tag": "lark_md", "content": f"**User:**\n{data.get('user_code')}"}},
                                {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Txn ID:**\n{data.get('txn_id')}"}},
                            ]
                        },
                        {"tag": "hr"},
                        {
                            "tag": "div",
                            "fields": [
                                {"is_short": True,  "text": {"tag": "lark_md", "content": f"**Trigger Type:**\n{decision}"}},
                                {"is_short": True,  "text": {"tag": "lark_md", "content": f"**System Actions:**\n{actions_str}"}},
                            ]
                        },
                        {"tag": "hr"},
                        {"tag": "div", "text": {"tag": "lark_md", "content": f"**Triggered Rule:**\n{rule_name}\n\n**Reasoning:**\n{data.get('narrative', '')}"}}
                    ]
                }
            }

        # 3. Fire Webhook
        req = urllib.request.Request(webhook_url, data=json.dumps(card_content).encode("utf-8"), headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=3)
        
        # 4. Audit Log (Reconciliation)
        if alert_id:
            try:
                conn = get_db_conn()
                cur = conn.cursor()
                cur.execute("UPDATE rt.risk_trade_alerts_v2 SET rendered_message = %s WHERE alert_id = %s", (final_rendered_msg, str(alert_id)))
                conn.commit()
                cur.close()
            except Exception as e:
                print(f"[URGENT DEBUG] Failed to log rendered message to Hologres: {e}")

    except Exception as e:
        print(f"[TRADE_RISK_V2_FC] Lark notification error: {e}")


# ==========================
# GATEWAY CLIENT — Automated Enforcement + Audit Log
# ==========================
def _pick_source_order_id(user_code, action, engine="TR"):
    """
    Choose a source_order_id for (user, action) that is:
      - STABLE while a lock is currently ACTIVE  -> re-fires update in place (no pileup)
      - FRESH after the previous lock is RELEASED/EXPIRED -> creates a NEW active lock
        (fixes "cannot re-apply after manual unlock")

    Mechanism (no live RDS; reads the ~4s Hologres replica, FAIL-OPEN toward enforcement):
      base = phalanx-{ENGINE}-{user}-{action}
      Read the newest lock row for (uid, action, source=PHALANX) whose id starts with base.
        - If newest is ACTIVE  -> reuse its exact source_order_id  (stable, in-place update)
        - else (RELEASED/EXPIRED/none) -> bump to base-v{N+1}       (new active lock)
      If the replica cannot be read -> return a fresh time-suffixed id (fail toward a NEW
      lock). Worst case under replica lag = one extra active row (bounded, operator-clearable,
      and self-healed by the Exchange's own idempotency when two calls pick the same version).
      This is the SAFE failure direction: we never suppress a needed lock.

    The Exchange dedups on (action, source, source_order_id) [confirmed UNIQUE index], so a
    reused id updates in place and a bumped id creates a new row — exactly what we want.
    """
    base = f"phalanx-{engine}-{user_code}-{action}"
    try:
        conn = get_db_conn()                 # shared Hologres conn (do NOT close)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT status, source_order_id
            FROM merchent_bullex.risk_permission_lock
            WHERE uid = %s AND action = %s AND source = 'PHALANX'
              AND (source_order_id = %s OR source_order_id LIKE %s)
            ORDER BY create_at DESC
            LIMIT 1
            """,
            (int(user_code) if str(user_code).isdigit() else user_code, action, base, base + '-v%'),
        )
        row = cur.fetchone()
        cur.close()

        if row is None:
            return base + "-v1"                      # never locked before
        status, newest_id = row[0], row[1]
        if str(status).upper() == "ACTIVE":
            return newest_id                          # reuse -> in-place update, no pileup
        # newest is RELEASED/EXPIRED -> start a new lifecycle
        # derive N from the suffix if present, else start at 2
        n = 1
        if isinstance(newest_id, str) and newest_id.startswith(base + "-v"):
            try:
                n = int(newest_id.rsplit("-v", 1)[1])
            except (ValueError, IndexError):
                n = 1
        return f"{base}-v{n + 1}"
    except Exception as exc:
        # FAIL-OPEN: replica unreadable -> mint a fresh, unique-ish id so the lock still applies.
        # Time suffix guarantees a new active lock; the Exchange idempotency dedups exact repeats.
        print(f"[TRADE_RISK_V2_FC] Version lookup failed (fail-open, applying fresh lock): {exc}")
        return f"{base}-t{int(time.time())}"


def execute_gateway_actions(user_code, rule_result, alert_id):
    import uuid # Ensure uuid is imported if not already at the top

    actions = rule_result.get("enforcement_actions", [])
    api_actions = [a for a in actions if a != "LARK_ALERT"]

    if not api_actions or not alert_id:
        return None

    ENGINE = "TR"  # this is the Trade FC; the Withdraw FC's copy sets "WD"

    # Per-action versioned idempotency id. Each action gets its own id keyed to its own
    # lock lifecycle, so releasing one action and re-triggering re-locks ONLY that action,
    # and a still-active action is updated in place (never stacked).
    per_action = []
    for act in api_actions:
        soid = _pick_source_order_id(user_code, act, ENGINE)
        per_action.append({"action": act, "source_order_id": soid,
                           "reason": rule_result.get("rule_name", "Risk Engine Trigger"),
                           "expire_seconds": 0})

    # The Exchange API takes ONE source_order_id per request. Because each action may now
    # carry a DIFFERENT version, we group actions by their chosen source_order_id and send
    # one request per distinct id. (Usually they share one id, so this is a single call.)
    from collections import defaultdict
    groups = defaultdict(list)
    for pa in per_action:
        groups[pa["source_order_id"]].append(pa)

    # Aggregate result across grouped calls (preserves the newly_applied/alert semantics).
    agg_status = None
    agg_newly_applied = False
    already_active = False
    agg_response_bodies = []
    any_sent = False

    for soid, acts in groups.items():
        payload = {
            "uid": int(user_code) if str(user_code).isdigit() else user_code,
            "source_order_id": soid,
            "alert_type": rule_result.get("alert_type", "Unknown"),
            "actions": [{"action": a["action"], "reason": a["reason"], "expire_seconds": a["expire_seconds"]} for a in acts]
        }
        res = _post_gateway_group(user_code, rule_result, alert_id, payload, soid)
        if res is None:
            continue
        any_sent = True
        if res.get("status") is not None:
            agg_status = res["status"] if agg_status is None else agg_status
        if res.get("newly_applied"):
            agg_newly_applied = True
        if res.get("response_body"):
            agg_response_bodies.append(res["response_body"])
        
        if res.get("already_active"):
            already_active = True

    if not any_sent:
        return None
    return {"status": agg_status, "shadow_mode": (not getattr(cfg, 'ENABLE_AUTOMATED_ACTIONS', False)),
            "skipped": False, "newly_applied": agg_newly_applied,"already_active": already_active}


def _post_gateway_group(user_code, rule_result, alert_id, payload, source_order_id):

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
            (audit_id, alert_id, user_code, rule_id, source_order_id, api_endpoint, payload_sent, hmac_signature, http_status_code, response_body, latency_ms, is_shadow_mode, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """,
            (audit_id, alert_id, str(user_code), rule_result.get("rule_id", 0), source_order_id, getattr(cfg, 'RISK_GATEWAY_URL', ''), json.dumps(payload), signature, http_status, response_body, latency, is_shadow)
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
    already_active = False
    if not is_shadow and response_body:
        try:
            resp_json = json.loads(response_body)
            data_list = resp_json.get("results") or resp_json.get("data") or []
            results = [str(i.get("Result") or i.get("result") or "").lower() for i in data_list]
            newly_applied = any(r == "applied" for r in results)
            already_active = bool(results) and all(r in ("updated",) for r in results)
        except Exception:
            pass

    return {"status": http_status, "shadow_mode": is_shadow, "skipped": False, "newly_applied": newly_applied,"already_active": already_active, "response_body": response_body}


def should_suppress_lark(user_code, rule_id):
    """Strict 30-minute (1800s) rolling debounce using OTS."""
    try:
        
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


def test_ots_connection():
    try:
        import os
        from tablestore import OTSClient

        endpoint = os.environ.get("OTS_ENDPOINT")
        instance = os.environ.get("OTS_INSTANCE")
        ak_id = os.environ.get("OTS_ACCESS_KEY_ID")
        ak_secret = os.environ.get("OTS_ACCESS_KEY_SECRET")

        print("Endpoint :", endpoint)
        print("Instance :", instance)
        print("AK ID    :", "YES" if ak_id else "NO")
        print("AK Secret:", "YES" if ak_secret else "NO")

        if not all([endpoint, instance, ak_id, ak_secret]):
            print("❌ Missing environment variables")
            return

        client = OTSClient(endpoint, ak_id, ak_secret, instance)

        table_name = "phalanx_alert_cache"
        primary_key = [("cache_key", "__health_check__")]

        try:
            _, row, _ = client.get_row(table_name, primary_key)
            print("✅ OTS Reachable")
            print("Row Exists:", row is not None)
        except Exception as e:
            print("❌ Connected to OTS but read failed")
            print(type(e).__name__, str(e))

    except Exception as e:
        print("❌ Failed to create OTS client")
        print(type(e).__name__, str(e))