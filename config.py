import os

DB_HOST = os.environ.get("DB_HOST", "hgpost-sg-...-vpc-st.hologres.aliyuncs.com")
DB_PORT = os.environ.get("DB_PORT", "80")
DB_NAME = os.environ.get("DB_NAME", "onebullex_rt")
DB_USER = os.environ.get("DB_USER", "BASIC$shafiq")
DB_PASS = os.environ.get("DB_PASS", "HOLOGRES@424")

LARK_WEBHOOK_URL = os.environ.get("LARK_WEBHOOK_URL", "")
RULE_CACHE_TTL = int(os.environ.get("RULE_CACHE_TTL", "60"))

# --- V2 Automated Enforcement Configs ---
# Toggle this to True to actually fire API requests. False = Shadow Mode (Audit logs only)
ENABLE_AUTOMATED_ACTIONS = os.getenv("ENABLE_AUTOMATED_ACTIONS", "True").lower() in ("true", "1", "yes")

RISK_GATEWAY_URL = os.getenv("RISK_GATEWAY_URL", "https://testadmin.1bullex.com/api/risk/action")
RISK_GATEWAY_API_KEY = os.getenv("RISK_GATEWAY_API_KEY", "70627b55544514b5c57638b7a0e67c3f")
RISK_GATEWAY_SECRET = os.getenv("RISK_GATEWAY_SECRET", "ceb6dbd900c08db5eb34d9a37868d51c8440071b7b95a19606917ae068a69d37")
RISK_GATEWAY_TIMEOUT = int(os.getenv("RISK_GATEWAY_TIMEOUT", "3")) # strict 3 second timeout