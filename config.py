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
#RISK_GATEWAY_URL = os.getenv("RISK_GATEWAY_URL", "https://testadmin.1bullex.com/api/risk/action")

ENABLE_AUTOMATED_ACTIONS = os.getenv("ENABLE_AUTOMATED_ACTIONS", "True").lower() in ("true", "1", "yes")
#RISK_GATEWAY_URL = os.getenv("RISK_GATEWAY_URL", "https://alb-dsg2mz9eq4ip750s4f.ap-northeast-1.alb.aliyuncsslbintl.com/api/risk/action")

RISK_GATEWAY_URL = os.getenv("RISK_GATEWAY_URL", "http://alb-dsg2mz9eq4ip750s4f.ap-northeast-1.alb.aliyuncsslbintl.com/api/risk/action")

RISK_GATEWAY_API_KEY = os.getenv("RISK_GATEWAY_API_KEY", "")
RISK_GATEWAY_SECRET = os.getenv("RISK_GATEWAY_SECRET", "")
RISK_GATEWAY_TIMEOUT = int(os.getenv("RISK_GATEWAY_TIMEOUT", "3")) # strict 3 second timeout

# --- ALERT ROUTING CONFIG ---
# --- ALERT ROUTING CONFIG (Trade) ---
ALERT_GROUPS = {
    "DEFAULT": LARK_WEBHOOK_URL,
    "PROFIT_AND_TRADING_RISK": "https://open.larksuite.com/open-apis/bot/v2/hook/7e394a25-7829-4d78-8967-ed52df3e880a",
    "ACCOUNT_SECURITY_RISK": "https://open.larksuite.com/open-apis/bot/v2/hook/5a701cc0-06aa-44e6-9407-a53c5cb415ec",
    "MARKET_MANIPULATION_WASHTRADING": "https://open.larksuite.com/open-apis/bot/v2/hook/e6ec276a-11a7-45e6-ac65-51c29374cf85",
    "LINKED_ACCOUNT_COORDINATION": "https://open.larksuite.com/open-apis/bot/v2/hook/e5c26112-caba-482b-9182-dd79102ce819",
    "REWARD_ARBITRAGE_ABUSE": "https://open.larksuite.com/open-apis/bot/v2/hook/9e77de7d-577c-4ca8-bc3a-bb75d3de4d41"
}