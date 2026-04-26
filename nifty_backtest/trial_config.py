# trial_config.py
# ──────────────────────────────────────────────────────────────
#  ▶  CHANGE THIS DATE BEFORE PUSHING TO GITHUB
#     Format: YYYY-MM-DD
#     Example: "2026-05-10" means trial expires end of May 10
# ──────────────────────────────────────────────────────────────

import base64

# Encode the expiry date so it's not plain text in the binary
# To change expiry: update the date string below and push
_RAW = "2026-05-05"
EXPIRY_ENCODED = base64.b64encode(_RAW.encode()).decode()

# Contact details shown on expiry
COMPANY_NAME = "Balfund Trading Private Limited"
CONTACT      = "+91 93543447"
PRODUCT_NAME = "NIFTY Straddle Backtest"
