#!/usr/bin/env python3
"""
Swing Trading Dashboard — Data Fetch Script (fetch.py)
======================================================
Runs daily via GitHub Actions at 3:30 PM IST.

1. Authenticates with Upstox via TOTP (zero manual intervention).
2. Fetches 1-year daily OHLCV for Nifty 50, Bank Nifty, India VIX,
   Nifty Midcap 50, and 10 sector indices.
3. Fetches ~50 days of daily data for all Nifty 500 constituents
   to compute real market breadth (% stocks above 20 EMA).
4. Computes 20 / 50 / 200 EMAs for each index.
5. Classifies overall market regime and per-sector regime.
6. Writes dashboard_data.json consumed by the GitHub Pages dashboard.

Environment variables (set as GitHub Secrets):
  UPSTOX_USERNAME      — Upstox registered mobile number (user ID)
  UPSTOX_PIN_CODE      — 6-digit login PIN
  UPSTOX_TOTP_SECRET   — TOTP secret key from Upstox authenticator setup
  UPSTOX_CLIENT_ID     — API app key (from Upstox Developer Portal)
  UPSTOX_CLIENT_SECRET — API app secret
  UPSTOX_REDIRECT_URI  — Redirect URI registered in Upstox app

Upstox login flow: Mobile number → TOTP (auto-generated) → 6-digit PIN
There is no separate password — TOTP serves as the authentication credential.

Note on upstox-totp library:
  The library still expects UPSTOX_PASSWORD in its env config.
  Set it to any non-empty placeholder (e.g. "x") — it's a legacy field
  and is not used in the current TOTP-based login flow.
"""

import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import quote

import requests

# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------

# --- Index instrument keys (Upstox format) ---

MARKET_SYMBOLS = {
    "NIFTY_50":   {"key": "NSE_INDEX|Nifty 50",        "label": "Nifty 50",        "type": "market"},
    "BANK_NIFTY": {"key": "NSE_INDEX|Nifty Bank",      "label": "Bank Nifty",      "type": "market"},
    "INDIA_VIX":  {"key": "NSE_INDEX|India VIX",        "label": "India VIX",       "type": "vix"},
    "MIDCAP":     {"key": "NSE_INDEX|Nifty Midcap 50",  "label": "Nifty Midcap 50", "type": "market"},
}

SECTOR_SYMBOLS = {
    "BANK":             {"key": "NSE_INDEX|Nifty Bank",               "label": "Nifty Bank"},
    "IT":               {"key": "NSE_INDEX|Nifty IT",                 "label": "Nifty IT"},
    "FIN_SERVICES":     {"key": "NSE_INDEX|Nifty Financial Services", "label": "Nifty Financial Services"},
    "AUTO":             {"key": "NSE_INDEX|Nifty Auto",               "label": "Nifty Auto"},
    "FMCG":             {"key": "NSE_INDEX|Nifty FMCG",              "label": "Nifty FMCG"},
    "METAL":            {"key": "NSE_INDEX|Nifty Metal",              "label": "Nifty Metal"},
    "PHARMA":           {"key": "NSE_INDEX|Nifty Pharma",             "label": "Nifty Pharma"},
    "REALTY":            {"key": "NSE_INDEX|Nifty Realty",             "label": "Nifty Realty"},
    "OIL_GAS":          {"key": "NSE_INDEX|Nifty Oil and Gas",       "label": "Nifty Oil & Gas"},
    "CONSUMER_DURABLES":{"key": "NSE_INDEX|Nifty Consumer Durables", "label": "Nifty Consumer Durables"},
}

# Merge unique instrument keys for index fetching
ALL_INDEX_KEYS: dict[str, str] = {}  # instrument_key -> friendly_id
for sid, meta in {**MARKET_SYMBOLS, **SECTOR_SYMBOLS}.items():
    ALL_INDEX_KEYS[meta["key"]] = sid

# Lookback periods
INDEX_LOOKBACK_DAYS = 365   # 1 year for index data (200 EMA warm-up)
BREADTH_LOOKBACK_DAYS = 50  # ~50 days for stocks (20 EMA warm-up)

# API base
UPSTOX_HIST_BASE = "https://api.upstox.com/v2/historical-candle"

# Retry and rate-limit config
MAX_RETRIES = 3
RETRY_DELAY = 2          # seconds between retries
INTER_CALL_DELAY = 0.25  # seconds between successive API calls
BREADTH_WORKERS = 5       # parallel threads for breadth stock fetches

# NSE India API for Nifty 500 constituents
NSE_BASE = "https://www.nseindia.com"
NSE_INDEX_API = f"{NSE_BASE}/api/equity-stockIndices"


# ---------------------------------------------------------------------------
# 2. AUTHENTICATION — Upstox TOTP
# ---------------------------------------------------------------------------

def get_access_token() -> str:
    """
    Obtain an Upstox access token using the upstox-totp library.
    Falls back to manual TOTP flow via requests if the library isn't available.
    """
    # --- Try the upstox-totp library first ---
    try:
        from upstox_totp import UpstoxTOTP
        print("[AUTH] Using upstox-totp library ...")
        upx = UpstoxTOTP()
        resp = upx.app_token.get_access_token()
        if resp.success and resp.data:
            print(f"[AUTH] Token obtained for user: {resp.data.user_id}")
            return resp.data.access_token
        else:
            print(f"[AUTH] upstox-totp failed: {getattr(resp, 'error', 'unknown')}")
            raise RuntimeError("upstox-totp token generation failed")
    except ImportError:
        print("[AUTH] upstox-totp not installed — falling back to manual TOTP flow")
    except Exception as e:
        print(f"[AUTH] upstox-totp error: {e} — falling back to manual TOTP flow")

    # --- Fallback: manual TOTP login ---
    return _manual_totp_login()


def _manual_totp_login() -> str:
    """
    Manual TOTP login flow using requests + pyotp.
    Upstox login: Mobile → TOTP → PIN (no password).
    """
    import pyotp

    username      = os.environ["UPSTOX_USERNAME"]
    pin           = os.environ["UPSTOX_PIN_CODE"]
    totp_secret   = os.environ["UPSTOX_TOTP_SECRET"]
    client_id     = os.environ["UPSTOX_CLIENT_ID"]
    client_secret = os.environ["UPSTOX_CLIENT_SECRET"]
    redirect_uri  = os.environ["UPSTOX_REDIRECT_URI"]

    session = requests.Session()
    totp = pyotp.TOTP(totp_secret)

    # Step 1 — Initiate auth dialog to get session cookies
    auth_url = (
        f"https://api.upstox.com/v2/login/authorization/dialog"
        f"?response_type=code&client_id={client_id}"
        f"&redirect_uri={quote(redirect_uri, safe='')}"
    )
    print("[AUTH] Step 1: Initiating auth dialog ...")
    session.get(auth_url, allow_redirects=True, timeout=30)

    # Step 2 — Submit mobile number + TOTP (TOTP is the credential, no password)
    print("[AUTH] Step 2: Submitting mobile + TOTP ...")
    r2 = session.post(
        "https://api.upstox.com/v2/login/authorization/step1",
        json={
            "mobileNumber": username,
            "otp": totp.now(),
        },
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )
    if r2.status_code != 200:
        raise RuntimeError(f"Step 1 login failed: {r2.status_code} {r2.text[:300]}")

    # Step 3 — Submit PIN
    print("[AUTH] Step 3: Submitting PIN ...")
    r3 = session.post(
        "https://api.upstox.com/v2/login/authorization/step2",
        json={"pin": pin},
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=30,
    )

    # Extract auth code from response body or redirect
    auth_code = None
    if r3.status_code in (200, 302):
        try:
            data = r3.json()
            auth_code = data.get("data", {}).get("code") or data.get("code")
        except Exception:
            pass
        if not auth_code and "Location" in r3.headers:
            from urllib.parse import parse_qs, urlparse
            loc = r3.headers["Location"]
            auth_code = parse_qs(urlparse(loc).query).get("code", [None])[0]

    if not auth_code:
        raise RuntimeError(f"No auth code. Status: {r3.status_code}, Body: {r3.text[:500]}")

    # Step 4 — Exchange code for access token
    print("[AUTH] Step 4: Exchanging code for token ...")
    r4 = requests.post(
        "https://api.upstox.com/v2/login/authorization/token",
        data={
            "code": auth_code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        },
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
        timeout=30,
    )
    if r4.status_code != 200:
        raise RuntimeError(f"Token exchange failed: {r4.status_code} {r4.text[:300]}")

    token = r4.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {r4.text[:300]}")

    print("[AUTH] Access token obtained")
    return token


# ---------------------------------------------------------------------------
# 3. DATA FETCHING — Index Historical Candles
# ---------------------------------------------------------------------------

def fetch_candles(
    instrument_key: str,
    access_token: str,
    from_date: str,
    to_date: str,
    interval: str = "day",
) -> list[list]:
    """
    Fetch historical candles from Upstox v2 API.
    Returns list of [timestamp, O, H, L, C, volume, OI] sorted oldest-first.
    """
    encoded_key = quote(instrument_key, safe="")
    url = f"{UPSTOX_HIST_BASE}/{encoded_key}/{interval}/{to_date}/{from_date}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200:
                data = r.json()
                candles = data.get("data", {}).get("candles", [])
                candles.reverse()  # API returns newest-first; flip to oldest-first
                return candles
            elif r.status_code == 429:
                wait = RETRY_DELAY * attempt
                time.sleep(wait)
            else:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return []


def fetch_all_index_data(access_token: str) -> dict[str, list[list]]:
    """Fetch 1-year daily candles for every required index."""
    from_date = (datetime.now() - timedelta(days=INDEX_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    result: dict[str, list[list]] = {}
    keys = list(ALL_INDEX_KEYS.keys())

    print(f"\n[DATA] Fetching {len(keys)} indices  ({from_date} to {to_date})\n")
    for i, inst_key in enumerate(keys, 1):
        friendly = ALL_INDEX_KEYS[inst_key]
        print(f"  [{i:2d}/{len(keys)}] {friendly:22s} ...", end=" ", flush=True)
        candles = fetch_candles(inst_key, access_token, from_date, to_date)
        result[inst_key] = candles
        status = f"{len(candles)} candles" if candles else "no data"
        print(status)
        if i < len(keys):
            time.sleep(INTER_CALL_DELAY)

    return result


# ---------------------------------------------------------------------------
# 4. NIFTY 500 BREADTH — Constituent Fetch + EMA Calculation
# ---------------------------------------------------------------------------

def _nse_session() -> requests.Session:
    """Create a requests session that can talk to NSE India APIs."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/market-data/live-equity-market",
    })
    # Hit homepage first to get session cookies
    s.get(NSE_BASE, timeout=15)
    time.sleep(0.5)
    return s


def fetch_nifty500_constituents() -> list[dict]:
    """
    Fetch current Nifty 500 constituent list from NSE India.
    Returns list of {"symbol": ..., "isin": ..., "upstox_key": ...}
    Falls back to Nifty 200 or Nifty 100 if 500 fails.
    """
    session = _nse_session()

    for index_name in ["NIFTY 500", "NIFTY 200", "NIFTY 100"]:
        try:
            print(f"  [BREADTH] Trying NSE API for {index_name} ...", end=" ", flush=True)
            r = session.get(
                NSE_INDEX_API,
                params={"index": index_name},
                timeout=20,
            )
            if r.status_code == 200:
                data = r.json()
                stocks = data.get("data", [])
                constituents = []
                for s in stocks:
                    isin = s.get("meta", {}).get("isin") or s.get("isin", "")
                    symbol = s.get("symbol", "")
                    if isin and symbol and isin.startswith("INE"):
                        constituents.append({
                            "symbol": symbol,
                            "isin": isin,
                            "upstox_key": f"NSE_EQ|{isin}",
                        })
                if constituents:
                    print(f"{len(constituents)} stocks")
                    session.close()
                    return constituents
                else:
                    print("empty list")
            else:
                print(f"HTTP {r.status_code}")
        except Exception as e:
            print(f"error: {e}")
        time.sleep(1)

    session.close()
    return []


def _fetch_stock_above_ema(
    args: tuple[str, str, str, str, str],
) -> tuple[str, bool | None]:
    """
    Worker: fetch candles for one stock and check if close > 20 EMA.
    Returns (symbol, True/False/None).
    """
    symbol, upstox_key, access_token, from_date, to_date = args
    try:
        candles = fetch_candles(upstox_key, access_token, from_date, to_date)
        if len(candles) < 20:
            return (symbol, None)

        closes = [c[4] for c in candles]

        # Compute 20 EMA inline (SMA seed + exponential smoothing)
        period = 20
        multiplier = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for c in closes[period:]:
            ema = (c - ema) * multiplier + ema

        return (symbol, closes[-1] > ema)
    except Exception:
        return (symbol, None)


def compute_market_breadth(
    constituents: list[dict],
    access_token: str,
) -> dict:
    """
    Compute real market breadth: % of stocks with close > 20 EMA.
    Uses ThreadPoolExecutor for parallel fetching.
    """
    from_date = (datetime.now() - timedelta(days=BREADTH_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    print(f"\n[BREADTH] Computing breadth for {len(constituents)} stocks "
          f"({from_date} to {to_date}, {BREADTH_WORKERS} threads)\n")

    tasks = [
        (c["symbol"], c["upstox_key"], access_token, from_date, to_date)
        for c in constituents
    ]

    above_count = 0
    below_count = 0
    error_count = 0
    total = len(tasks)

    with ThreadPoolExecutor(max_workers=BREADTH_WORKERS) as executor:
        futures = {executor.submit(_fetch_stock_above_ema, t): t[0] for t in tasks}
        done = 0
        for future in as_completed(futures):
            done += 1
            symbol, result = future.result()
            if result is True:
                above_count += 1
            elif result is False:
                below_count += 1
            else:
                error_count += 1

            # Progress every 50 stocks
            if done % 50 == 0 or done == total:
                computed = above_count + below_count
                pct = (above_count / computed * 100) if computed > 0 else 0
                print(
                    f"  [BREADTH] {done:3d}/{total} fetched | "
                    f"Above: {above_count} | Below: {below_count} | "
                    f"Errors: {error_count} | Running: {pct:.1f}%"
                )

            # Small delay per thread to stay within rate limits
            time.sleep(INTER_CALL_DELAY / BREADTH_WORKERS)

    computed = above_count + below_count
    breadth_pct = (above_count / computed * 100) if computed > 0 else None

    return {
        "breadth_pct": round(breadth_pct, 1) if breadth_pct is not None else None,
        "above_ema": above_count,
        "below_ema": below_count,
        "errors": error_count,
        "total_constituents": total,
        "total_computed": computed,
        "index_used": f"NIFTY {total}" if total >= 400 else f"~{total} stocks",
    }


# ---------------------------------------------------------------------------
# 5. TECHNICAL CALCULATIONS (for indices)
# ---------------------------------------------------------------------------

def calc_ema(closes: list[float], period: int) -> list[float | None]:
    """EMA with SMA seed. Returns list same length as closes."""
    if len(closes) < period:
        return [None] * len(closes)

    multiplier = 2 / (period + 1)
    emas: list[float | None] = [None] * (period - 1)

    sma = sum(closes[:period]) / period
    emas.append(round(sma, 2))

    for i in range(period, len(closes)):
        val = (closes[i] - emas[-1]) * multiplier + emas[-1]
        emas.append(round(val, 2))

    return emas


def calc_atr(candles: list[list], period: int = 14) -> float | None:
    """Latest ATR(period) using Wilder's smoothing."""
    if len(candles) < period + 1:
        return None

    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i][2], candles[i][3], candles[i - 1][4]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period

    return round(atr, 2)


def process_symbol(candles: list[list]) -> dict:
    """Compute EMAs and summary stats for one symbol's candle data."""
    if not candles:
        return {
            "current_close": None, "prev_close": None, "change_pct": None,
            "ema_20": None, "ema_50": None, "ema_200": None, "atr_14": None,
            "high_52w": None, "low_52w": None, "candle_count": 0,
        }

    closes = [c[4] for c in candles]
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)
    atr = calc_atr(candles)

    current = closes[-1]
    prev = closes[-2] if len(closes) >= 2 else None
    change = round(((current - prev) / prev) * 100, 2) if prev and prev != 0 else None

    return {
        "current_close": current,
        "prev_close": prev,
        "change_pct": change,
        "ema_20": ema20[-1] if ema20 and ema20[-1] is not None else None,
        "ema_50": ema50[-1] if ema50 and ema50[-1] is not None else None,
        "ema_200": ema200[-1] if ema200 and ema200[-1] is not None else None,
        "atr_14": atr,
        "high_52w": round(max(c[2] for c in candles), 2),
        "low_52w": round(min(c[3] for c in candles), 2),
        "candle_count": len(candles),
    }


# ---------------------------------------------------------------------------
# 6. REGIME CLASSIFICATION
# ---------------------------------------------------------------------------

def classify_market_regime(
    nifty: dict,
    vix: dict,
    breadth_pct: float | None,
    sector_regimes: dict,
) -> dict:
    """
    Classify overall market regime using Nifty EMAs + VIX + breadth.

    Uses real breadth (% of Nifty 500 above 20 EMA) when available,
    falls back to sector-based proxy otherwise.

    Regime rules:
      Bullish Trending — Nifty > 20 & 50 EMA, breadth > 60%, VIX < 16
      Sideways         — Nifty near 20 EMA (+/-1%), breadth 40-60%
      Recovering       — Nifty < 20 EMA but > 50 EMA, breadth rising
      Correcting       — Nifty < 20 & 50 EMA, breadth < 40%
      Bear             — Nifty < 200 EMA, VIX > 22
    """
    close = nifty.get("current_close")
    ema20 = nifty.get("ema_20")
    ema50 = nifty.get("ema_50")
    ema200 = nifty.get("ema_200")
    vix_close = vix.get("current_close")

    if any(v is None for v in [close, ema20, ema50, ema200]):
        return {
            "regime": "Unknown", "active_modules": [],
            "description": "Insufficient data for classification", "colour": "gray",
        }

    # Breadth: prefer real, fall back to sector proxy
    if breadth_pct is not None:
        breadth = breadth_pct
        bsrc = "Nifty 500"
    else:
        total = len(sector_regimes)
        bullish = sum(1 for s in sector_regimes.values() if s.get("regime") == "Bullish")
        breadth = (bullish / total * 100) if total > 0 else 50
        bsrc = "sector proxy"

    vix_val = vix_close if vix_close is not None else 15

    # --- Classification (most restrictive first) ---

    if close < ema200 and vix_val > 22:
        return {
            "regime": "Bear", "active_modules": [],
            "description": f"Nifty below 200 EMA, VIX {vix_val:.1f}. Breadth {breadth:.0f}% ({bsrc})",
            "colour": "red",
        }

    if close < ema20 and close < ema50:
        return {
            "regime": "Correcting", "active_modules": ["M2"],
            "description": f"Nifty below 20 & 50 EMA. Breadth {breadth:.0f}% ({bsrc})",
            "colour": "orange",
        }

    if close < ema20 and close >= ema50:
        return {
            "regime": "Recovering", "active_modules": ["M2", "M3"],
            "description": f"Nifty below 20 EMA, above 50 EMA. Breadth {breadth:.0f}% ({bsrc})",
            "colour": "yellow",
        }

    # Close >= ema20 from here
    pct_from_ema20 = abs(close - ema20) / ema20 * 100

    if pct_from_ema20 <= 1.0 and 40 <= breadth <= 60:
        return {
            "regime": "Sideways", "active_modules": ["M3"],
            "description": f"Nifty within 1% of 20 EMA. Breadth {breadth:.0f}% ({bsrc})",
            "colour": "yellow",
        }

    if close > ema20 and close > ema50 and breadth > 60 and vix_val < 16:
        return {
            "regime": "Bullish Trending", "active_modules": ["M1", "M3"],
            "description": f"Nifty above 20 & 50 EMA, VIX {vix_val:.1f}, breadth {breadth:.0f}% ({bsrc})",
            "colour": "green",
        }

    # Above EMAs but breadth or VIX not fully qualifying
    if close > ema20 and close > ema50:
        notes = []
        if vix_val >= 16:
            notes.append(f"VIX elevated ({vix_val:.1f})")
        if breadth <= 60:
            notes.append(f"breadth moderate ({breadth:.0f}%)")
        detail = "; ".join(notes) if notes else "all clear"
        return {
            "regime": "Bullish Trending", "active_modules": ["M1", "M3"],
            "description": f"Nifty above 20 & 50 EMA. {detail}. ({bsrc})",
            "colour": "green",
        }

    # Fallback
    return {
        "regime": "Sideways", "active_modules": ["M3"],
        "description": f"Mixed signals. Breadth {breadth:.0f}% ({bsrc})",
        "colour": "yellow",
    }


def classify_sector_regime(data: dict) -> dict:
    """
    Classify a sector for position sizing.
      Bullish    — above 20 & 50 EMA -> 100%
      Recovering — below 20, above 50 -> 50%
      Sideways   — within 2% of 20 EMA -> 50%
      Correcting — below 20 & 50 EMA -> skip
    """
    close = data.get("current_close")
    ema20 = data.get("ema_20")
    ema50 = data.get("ema_50")

    if any(v is None for v in [close, ema20, ema50]):
        return {"regime": "Unknown", "position_size_rule": "Skip — insufficient data", "colour": "gray"}

    if close > ema20 and close > ema50:
        return {"regime": "Bullish", "position_size_rule": "100% of calculated size", "colour": "green"}
    elif close <= ema20 and close > ema50:
        return {"regime": "Recovering", "position_size_rule": "50% of calculated size", "colour": "yellow"}
    elif abs(close - ema20) / ema20 * 100 <= 2.0:
        return {"regime": "Sideways", "position_size_rule": "50% of calculated size", "colour": "yellow"}
    else:
        return {"regime": "Correcting", "position_size_rule": "Skip trade entirely", "colour": "orange"}


# ---------------------------------------------------------------------------
# 7. JSON OUTPUT
# ---------------------------------------------------------------------------

def build_dashboard_json(
    raw_index_data: dict[str, list[list]],
    breadth_data: dict | None,
) -> dict:
    """Build the complete dashboard_data.json."""
    now = datetime.now()

    # --- Process market symbols ---
    market_processed = {}
    for sid, meta in MARKET_SYMBOLS.items():
        candles = raw_index_data.get(meta["key"], [])
        processed = process_symbol(candles)
        market_processed[sid] = {**processed, "label": meta["label"], "instrument_key": meta["key"]}

    # --- Process sector symbols ---
    sector_processed = {}
    sector_regimes = {}
    for sid, meta in SECTOR_SYMBOLS.items():
        candles = raw_index_data.get(meta["key"], [])
        processed = process_symbol(candles)
        regime = classify_sector_regime(processed)
        sector_processed[sid] = {
            **processed,
            "label": meta["label"],
            "instrument_key": meta["key"],
            "regime": regime["regime"],
            "position_size_rule": regime["position_size_rule"],
            "regime_colour": regime["colour"],
        }
        sector_regimes[sid] = regime

    # --- Market regime ---
    nifty = market_processed.get("NIFTY_50", {})
    vix = market_processed.get("INDIA_VIX", {})
    real_breadth = breadth_data.get("breadth_pct") if breadth_data else None
    market_regime = classify_market_regime(nifty, vix, real_breadth, sector_regimes)

    # --- Assemble output ---
    return {
        "meta": {
            "generated_at": now.isoformat(),
            "generated_date": now.strftime("%Y-%m-%d"),
            "generated_time": now.strftime("%H:%M:%S"),
            "data_source": "Upstox API v2",
            "index_lookback_days": INDEX_LOOKBACK_DAYS,
            "breadth_lookback_days": BREADTH_LOOKBACK_DAYS,
            "schema_version": "1.1",
        },
        "market_pulse": {
            "nifty": market_processed.get("NIFTY_50"),
            "bank_nifty": market_processed.get("BANK_NIFTY"),
            "midcap": market_processed.get("MIDCAP"),
            "vix": market_processed.get("INDIA_VIX"),
        },
        "market_regime": market_regime,
        "breadth": breadth_data or {
            "breadth_pct": None, "above_ema": None, "below_ema": None,
            "errors": None, "total_constituents": None,
            "total_computed": None, "index_used": "unavailable",
        },
        "sector_regimes": sector_processed,
        "modules": {
            "M1": {
                "name": "Trend Pullback",
                "active_in": ["Bullish Trending"],
                "entry": "Strong stock in uptrend pulling back to 20 EMA. Entry on bullish candle bounce.",
                "stop": "Entry - (1.5 x ATR14)",
                "target": "2.5x risk distance",
                "hold": "5-15 days",
            },
            "M2": {
                "name": "Oversold Bounce",
                "active_in": ["Correcting", "Recovering"],
                "entry": "Quality stock above 200 EMA, RSI < 38, down 8%+ in 5 days. First green candle (body > 50%).",
                "stop": "Entry - (1.5 x ATR14)",
                "target": "5-8% or prior resistance",
                "hold": "2-7 days",
            },
            "M3": {
                "name": "Range Breakout",
                "active_in": ["Bullish Trending", "Sideways", "Recovering"],
                "entry": "8-12% range for 15-30 sessions, volume drying up. Close above range high, vol >= 2x 20d avg.",
                "stop": "Entry - (1.5 x ATR14)",
                "target": "3x risk distance",
                "hold": "5-21 days",
            },
        },
        "risk_rules": {
            "max_risk_per_trade": "0.5% of capital",
            "max_open_trades": 5,
            "max_portfolio_risk": "2.5% of capital",
            "stop_loss_method": "Entry - (1.5 x ATR14)",
            "time_stop": "Exit if no movement in 10 sessions",
            "loss_streak_rule": "3 consecutive losses in a week -> stop that week",
            "monthly_drawdown": "Down 5% -> reduce sizes by 50% next month",
        },
    }


# ---------------------------------------------------------------------------
# 8. MAIN
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()

    print("=" * 65)
    print("  Swing Trading Dashboard — Data Fetch")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # -- Step 1: Authenticate --
    print("\n[STEP 1/5] Authenticating with Upstox ...")
    try:
        access_token = get_access_token()
    except Exception as e:
        print(f"\n[FATAL] Authentication failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    # -- Step 2: Fetch index data --
    print("\n[STEP 2/5] Fetching index historical data ...")
    try:
        raw_index_data = fetch_all_index_data(access_token)
    except Exception as e:
        print(f"\n[FATAL] Index data fetch failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    nifty_key = MARKET_SYMBOLS["NIFTY_50"]["key"]
    if not raw_index_data.get(nifty_key):
        print("\n[FATAL] No data for Nifty 50 — cannot proceed")
        sys.exit(1)

    # -- Step 3: Fetch Nifty 500 constituents --
    print("\n[STEP 3/5] Fetching Nifty 500 constituent list from NSE ...")
    try:
        constituents = fetch_nifty500_constituents()
    except Exception as e:
        print(f"  [BREADTH] Failed to get constituents: {e}")
        constituents = []

    # -- Step 4: Compute breadth --
    breadth_data = None
    if constituents:
        print(f"\n[STEP 4/5] Computing market breadth ({len(constituents)} stocks) ...")
        try:
            breadth_data = compute_market_breadth(constituents, access_token)
            pct = breadth_data["breadth_pct"]
            above = breadth_data["above_ema"]
            comp = breadth_data["total_computed"]
            print(f"\n  [BREADTH] Final: {pct}% ({above}/{comp} above 20 EMA)")
        except Exception as e:
            print(f"  [BREADTH] Breadth computation failed: {e}")
            traceback.print_exc()
    else:
        print("\n[STEP 4/5] Skipping breadth (no constituent list) — will use sector proxy")

    # -- Step 5: Process and write JSON --
    print("\n[STEP 5/5] Processing data, classifying regimes, writing JSON ...")
    dashboard = build_dashboard_json(raw_index_data, breadth_data)

    output_path = os.environ.get("OUTPUT_PATH", "dashboard_data.json")
    with open(output_path, "w") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)

    # -- Summary --
    elapsed = time.time() - start_time
    regime = dashboard["market_regime"]["regime"]
    modules = ", ".join(dashboard["market_regime"]["active_modules"]) or "None"
    nifty_close = dashboard["market_pulse"]["nifty"]["current_close"]
    vix_close = dashboard["market_pulse"]["vix"]["current_close"]
    breadth_val = dashboard["breadth"]["breadth_pct"]

    print(f"\n{'=' * 65}")
    print(f"  Done in {elapsed:.1f}s  |  Output: {output_path}")
    print(f"")
    print(f"  Nifty 50:       {nifty_close}")
    print(f"  India VIX:      {vix_close}")
    if breadth_val is not None:
        print(f"  Breadth:        {breadth_val}% of Nifty 500 above 20 EMA")
    else:
        print(f"  Breadth:        unavailable (using sector proxy)")
    print(f"  Market Regime:  {regime}")
    print(f"  Active Modules: {modules}")
    print(f"")
    print(f"  Sector Regimes:")
    for sid, sdata in dashboard["sector_regimes"].items():
        print(f"    {sdata['label']:30s}  {sdata['regime']:12s}  {sdata['position_size_rule']}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
