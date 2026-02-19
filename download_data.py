import os
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from SmartApi import SmartConnect
import pyotp


# Fill in your credentials here 
API_KEY    = ""
CLIENT_ID  = ""    
PASSWORD   = ""
TOTP_SECRET = "" 


# ── Authenticate
print("Authenticating...")
obj = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
data = obj.generateSession(CLIENT_ID, PASSWORD, totp)

if not data["status"]:
    print("Authentication failed:", data)
    sys.exit(1)

print("Authentication successful!\n")


SYMBOLS = {
    "NIFTY":     {"token": "99926000", "exchange": "NSE"},
    "BANKNIFTY": {"token": "99926009", "exchange": "NSE"},
}

# ── Date range ──────────────────────────────────────────────
START = datetime(2023, 1, 1)
END   = datetime(2024, 12, 31)
INTERVAL = "FIVE_MINUTE"
CHUNK_DAYS = 30   # Angel One allows max ~30 days per request

os.makedirs("data", exist_ok=True)

def fetch_chunk(token, exchange, from_dt, to_dt, symbol):
    """Fetch one chunk of candle data."""
    params = {
        "exchange":    exchange,
        "symboltoken": token,
        "interval":    INTERVAL,
        "fromdate":    from_dt.strftime("%Y-%m-%d %H:%M"),
        "todate":      to_dt.strftime("%Y-%m-%d %H:%M"),
    }
    try:
        time.sleep(0.5)  # rate limit
        resp = obj.getCandleData(params)
        if resp["status"] and resp["data"]:
            df = pd.DataFrame(resp["data"],
                              columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            print(f"  {from_dt.date()} → {to_dt.date()} : {len(df)} rows")
            return df
        else:
            print(f"  {from_dt.date()} → {to_dt.date()} : empty response")
            return None
    except Exception as e:
        print(f"  Error: {e}")
        return None

for symbol, info in SYMBOLS.items():
    print(f"Downloading {symbol}...")
    token    = info["token"]
    exchange = info["exchange"]
    all_dfs  = []
    current  = START

    while current < END:
        chunk_end = min(current + timedelta(days=CHUNK_DAYS), END)
        df = fetch_chunk(token, exchange, current, chunk_end, symbol)
        if df is not None:
            all_dfs.append(df)
        current = chunk_end + timedelta(minutes=5)

    if all_dfs:
        final = pd.concat(all_dfs).drop_duplicates("timestamp").sort_values("timestamp")
        out_path = f"data/{symbol}_5min.csv"
        final.to_csv(out_path, index=False)
        print(f"\n  ✓ Saved {len(final):,} rows → {out_path}\n")
    else:
        print(f"\n  ✗ No data saved for {symbol}\n")
        print("  Trying alternate tokens...")
        # Fallback: try the older token format
        ALT_TOKENS = {
            "NIFTY":     [("26000", "NSE"), ("26000", "NFO"), ("99926000", "NFO")],
            "BANKNIFTY": [("26009", "NSE"), ("26009", "NFO"), ("99926009", "NFO")],
        }
        for alt_token, alt_exchange in ALT_TOKENS.get(symbol, []):
            print(f"  Trying token={alt_token} exchange={alt_exchange}...")
            df = fetch_chunk(alt_token, alt_exchange, 
                             datetime(2024, 1, 2, 9, 15),  # just test 1 day
                             datetime(2024, 1, 2, 15, 30), symbol)
            if df is not None and len(df) > 0:
                print(f"  ✓ Found working combination: token={alt_token} exchange={alt_exchange}")
                print(f"  Update SYMBOLS dict with these values and re-run.")
                break

print("Done!")