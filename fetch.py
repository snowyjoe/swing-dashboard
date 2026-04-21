#!/usr/bin/env python3
"""
Swing Trading Dashboard — Data Fetch Script (fetch.py)  v1.3
=============================================================
Runs daily via GitHub Actions at 3:30 PM IST.

1. Authenticates with Upstox via TOTP (zero manual intervention).
2. Fetches 1-year daily OHLCV for Nifty 50, Bank Nifty, India VIX,
   Nifty Midcap 50, and 10 sector indices.
3. Fetches ~50 days of daily data for all Nifty 500 constituents
   to compute real market breadth (% stocks above 20 EMA).
4. Computes 20 / 50 / 200 EMAs for each index.
5. Classifies overall market regime and per-sector regime.
6. Computes decision layer: regime transition, insight, playbook, scan here.
7. Writes dashboard_data.json consumed by the GitHub Pages dashboard.
8. Appends today's regime + breadth to rolling history files.

Environment variables (set as GitHub Secrets):
  UPSTOX_USERNAME      — Upstox registered mobile number (user ID)
  UPSTOX_PIN_CODE      — 6-digit login PIN
  UPSTOX_TOTP_SECRET   — TOTP secret key from Upstox authenticator setup
  UPSTOX_CLIENT_ID     — API app key (from Upstox Developer Portal)
  UPSTOX_CLIENT_SECRET — API app secret
  UPSTOX_REDIRECT_URI  — Redirect URI registered in Upstox app

Optional output paths:
  OUTPUT_PATH           — default: dashboard_data.json
  REGIME_HISTORY_PATH   — default: regime_history.json
  BREADTH_HISTORY_PATH  — default: breadth_history.json

Upstox login flow: Mobile number → TOTP (auto-generated) → 6-digit PIN
There is no separate password — TOTP serves as the authentication credential.

Note on upstox-totp library:
  The library still expects UPSTOX_PASSWORD in its env config.
  Set it to any non-empty placeholder (e.g. "x") — it's a legacy field
  and is not used in the current TOTP-based login flow.

Note on cron timing (VIX N-1 data lag):
  If VIX is one day behind, shift the workflow cron to 16:30 or 17:00 IST.
  Upstox sometimes finalises VIX EOD candles slightly after 15:30.
"""

import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests

# ---------------------------------------------------------------------------
# 1. CONFIGURATION
# ---------------------------------------------------------------------------

# --- Desired indices: friendly_id → search terms + label ---
# Search terms use the EXACT Upstox lowercase names so exact-match hits on
# the first pass. Substring fallback is kept only for future name changes.
# Based on real names from Upstox instruments file (see log for full list).

MARKET_SYMBOLS_WANTED = {
    "NIFTY_50":   {"search": ["Nifty 50"],         "label": "Nifty 50",        "type": "market"},
    "BANK_NIFTY": {"search": ["Nifty Bank"],       "label": "Bank Nifty",      "type": "market"},
    "INDIA_VIX":  {"search": ["India VIX"],        "label": "India VIX",       "type": "vix"},
    "MIDCAP":     {"search": ["Nifty Midcap 50"],  "label": "Nifty Midcap 50", "type": "market"},
}

SECTOR_SYMBOLS_WANTED = {
    "BANK":              {"search": ["Nifty Bank"],          "label": "Nifty Bank"},
    "IT":                {"search": ["Nifty IT"],            "label": "Nifty IT"},
    "FIN_SERVICES":      {"search": ["Nifty Fin Service"],   "label": "Nifty Fin Services"},
    "AUTO":              {"search": ["Nifty Auto"],          "label": "Nifty Auto"},
    "FMCG":              {"search": ["Nifty FMCG"],          "label": "Nifty FMCG"},
    "METAL":             {"search": ["Nifty Metal"],         "label": "Nifty Metal"},
    "PHARMA":            {"search": ["Nifty Pharma"],        "label": "Nifty Pharma"},
    "REALTY":            {"search": ["Nifty Realty"],        "label": "Nifty Realty"},
    "OIL_GAS":           {"search": ["Nifty Oil and Gas"],   "label": "Nifty Oil & Gas"},
    "CONSUMER_DURABLES": {"search": ["Nifty Consr Durbl"],   "label": "Nifty Consumer Durables"},
}

# These dicts get populated at runtime by resolve_instrument_keys()
MARKET_SYMBOLS: dict = {}
SECTOR_SYMBOLS: dict = {}
ALL_INDEX_KEYS: dict[str, str] = {}

# Lookback periods
INDEX_LOOKBACK_DAYS = 365   # 1 year for index data (200 EMA warm-up)
BREADTH_LOOKBACK_DAYS = 50  # ~50 days for stocks (20 EMA warm-up)

# API base
UPSTOX_HIST_BASE = "https://api.upstox.com/v2/historical-candle"

# Retry and rate-limit config
MAX_RETRIES = 3
RETRY_DELAY = 2
INTER_CALL_DELAY = 0.25
BREADTH_WORKERS = 5

# NSE India API for Nifty 500 constituents
NSE_BASE = "https://www.nseindia.com"
NSE_INDEX_API = f"{NSE_BASE}/api/equity-stockIndices"

# Upstox instruments file
UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"

# History file caps
HISTORY_KEEP = 20

# ---------------------------------------------------------------------------
# 1a. DECISION LAYER — static mappings
# ---------------------------------------------------------------------------

REGIME_COLOUR = {
    "Bullish Trending": "bull",
    "Recovering":       "recov",
    "Sideways":         "side",
    "Correcting":       "correct",
    "Bear":             "bear",
    "Unknown":          "side",
}
REGIME_LABEL = {
    "Bullish Trending": "BULL",
    "Recovering":       "RECOV",
    "Sideways":         "SIDE",
    "Correcting":       "CORR",
    "Bear":             "BEAR",
    "Unknown":          "?",
}
REGIME_RANK = {
    "Bear": 0, "Correcting": 1, "Recovering": 2,
    "Sideways": 3, "Bullish Trending": 4, "Unknown": 2,
}

# Module allocation per regime: (module, priority, friendly_name)
MODULE_MAP = {
    "Bullish Trending": [("M1", "primary", "Trend Pullback"),
                         ("M3", "secondary", "Range Breakout")],
    "Sideways":         [("M3", "primary", "Range Breakout")],
    "Recovering":       [("M2", "primary", "Oversold Bounce"),
                         ("M3", "secondary", "Range Breakout")],
    "Correcting":       [("M2", "primary", "Oversold Bounce")],
    "Bear":             [],
    "Unknown":          [],
}
SKIP_MAP = {
    "Bullish Trending": [("M2", "Oversold Bounce")],
    "Sideways":         [("M1", "Trend Pullback"), ("M2", "Oversold Bounce")],
    "Recovering":       [("M1", "Trend Pullback")],
    "Correcting":       [("M1", "Trend Pullback"), ("M3", "Range Breakout")],
    "Bear":             [("M1", "Trend Pullback"), ("M2", "Oversold Bounce"),
                         ("M3", "Range Breakout")],
    "Unknown":          [],
}

# Today's Mode — transition.status → (mode, wait_for, dont)
# The single most important block a trader reads in the morning.
TODAY_MODE_MAP = {
    "FRESH BULL":    ("TRADE — half size",  "Pullbacks to 20 EMA in ready sectors",          "Chase extended sectors, anticipate breakouts"),
    "STABLE BULL":   ("TRADE — full size",  "Clean setups in ready sectors",                 "Buy tops, force M1 where no pullback"),
    "IMPROVING":     ("TRADE — half size",  "Confirmation of new uptrend",                   "Full size until trend proves itself"),
    "WEAKENING":     ("REDUCE — half size", "Existing positions to hit targets",             "New entries, add to losers"),
    "CHOPPY":        ("STAND ASIDE",        "Regime to settle (2+ days same state)",         "Any new entries today"),
    "STABLE SIDE":   ("SELECTIVE — M3 only","Clean range breakouts with volume",             "M1 pullbacks, anticipate direction"),
    "STABLE RECOV":  ("TRADE — half size",  "M2 setups in oversold leaders",                 "M1 until Bullish confirms"),
    "STABLE CORR":   ("M2 ONLY — half size","Deep oversold bounces in strong names",         "M1 or M3 in this regime"),
    "STABLE BEAR":   ("NO TRADES",          "Regime to shift to Correcting or better",       "Catching knives"),
}
# Default when status doesn't match (early days of new regime, etc.)
TODAY_MODE_DEFAULT = ("OBSERVE", "Regime to stabilise before committing", "Early conviction trades")

# Insight fallback — when no rules fire, show regime-appropriate guidance
# so the Insight block never says generic "no significant changes".
INSIGHT_FALLBACK_MAP = {
    "FRESH BULL":    "Day 1 confirmation — wait for strong closes, don't anticipate.",
    "STABLE BULL":   "Trend intact — stick to the system.",
    "IMPROVING":     "Regime climbing — re-engage cautiously, half size.",
    "WEAKENING":     "Leadership thinning — tighten stops, no new longs.",
    "CHOPPY":        "Regime flipping — stand aside until it settles.",
    "STABLE SIDE":   "Range-bound — M3 breakouts only, with volume.",
    "STABLE RECOV":  "Under 20 EMA but above 50 — M2 zone, size half.",
    "STABLE CORR":   "Correction intact — M2 oversold bounces only.",
    "STABLE BEAR":   "Bear regime — no trades.",
}

# Scan Here thresholds (% distance from 20 EMA)
SCAN_READY_MAX = 3.0   # 0% to +3% → READY for M1/M3
# Above SCAN_READY_MAX → EXTENDED (don't chase)
# Below 0%              → WEAK (M2 candidates)


# ---------------------------------------------------------------------------
# 1b. INSTRUMENT KEY RESOLUTION
# ---------------------------------------------------------------------------

def resolve_instrument_keys():
    """
    Download the Upstox instruments JSON and resolve the correct
    instrument_key for every index we need.

    Strategy:
      Pass 1 — exact case-insensitive name match for every search term
               across every wanted symbol. This is the reliable path.
      Pass 2 — substring fallback, used only if exact match failed.
               Kept conservative to avoid near-miss hits like
               'nifty finserexbnk' matching 'Nifty Fin'.
    """
    global MARKET_SYMBOLS, SECTOR_SYMBOLS, ALL_INDEX_KEYS

    print("\n[KEYS] Downloading Upstox instruments file ...")

    nse_indices: list[dict] = []
    try:
        import gzip

        r = requests.get(UPSTOX_INSTRUMENTS_URL, timeout=30)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")

        try:
            raw = gzip.decompress(r.content)
        except Exception:
            raw = r.content

        all_instruments = json.loads(raw)
        nse_indices = [i for i in all_instruments if i.get("segment") == "NSE_INDEX"]
        print(f"[KEYS] Found {len(nse_indices)} NSE_INDEX instruments")

    except Exception as e:
        print(f"[KEYS] ✗ Failed to download instruments: {e}")
        print("[KEYS] Falling back to hardcoded keys (some may be wrong)")
        _use_hardcoded_keys()
        return

    # Build lookup: lowercase name → instrument record
    lookup: dict[str, dict] = {}
    for inst in nse_indices:
        name = inst.get("name", "") or inst.get("trading_symbol", "")
        if name:
            lookup[name.lower()] = inst

    print(f"[KEYS] Available indices: {', '.join(sorted(lookup.keys()))}")

    def _find(search_terms: list[str]) -> dict | None:
        # Pass 1: exact matches (prefer this, most deterministic)
        for term in search_terms:
            t = term.lower()
            if t in lookup:
                return lookup[t]
        # Pass 2: substring fallback
        for term in search_terms:
            t = term.lower()
            for name, inst in lookup.items():
                if t in name:
                    return inst
        return None

    MARKET_SYMBOLS = {}
    for sid, wanted in MARKET_SYMBOLS_WANTED.items():
        inst = _find(wanted["search"])
        if inst:
            key = inst["instrument_key"]
            MARKET_SYMBOLS[sid] = {"key": key, "label": wanted["label"],
                                   "type": wanted.get("type", "market")}
            print(f"[KEYS]   {sid:22s} → {key}")
        else:
            print(f"[KEYS]   {sid:22s} → ✗ NOT FOUND (searched: {wanted['search']})")

    SECTOR_SYMBOLS = {}
    for sid, wanted in SECTOR_SYMBOLS_WANTED.items():
        inst = _find(wanted["search"])
        if inst:
            key = inst["instrument_key"]
            SECTOR_SYMBOLS[sid] = {"key": key, "label": wanted["label"]}
            print(f"[KEYS]   {sid:22s} → {key}")
        else:
            print(f"[KEYS]   {sid:22s} → ✗ NOT FOUND (searched: {wanted['search']})")

    ALL_INDEX_KEYS.clear()
    for sid, meta in {**MARKET_SYMBOLS, **SECTOR_SYMBOLS}.items():
        ALL_INDEX_KEYS[meta["key"]] = sid

    found = len(MARKET_SYMBOLS) + len(SECTOR_SYMBOLS)
    wanted = len(MARKET_SYMBOLS_WANTED) + len(SECTOR_SYMBOLS_WANTED)
    print(f"\n[KEYS] Resolved {found}/{wanted} instrument keys")


def _use_hardcoded_keys():
    """Fallback: best-guess hardcoded keys if instruments file is unavailable."""
    global MARKET_SYMBOLS, SECTOR_SYMBOLS, ALL_INDEX_KEYS

    MARKET_SYMBOLS = {
        "NIFTY_50":   {"key": "NSE_INDEX|Nifty 50",        "label": "Nifty 50",        "type": "market"},
        "BANK_NIFTY": {"key": "NSE_INDEX|Nifty Bank",      "label": "Bank Nifty",      "type": "market"},
        "INDIA_VIX":  {"key": "NSE_INDEX|India VIX",       "label": "India VIX",       "type": "vix"},
        "MIDCAP":     {"key": "NSE_INDEX|Nifty Midcap 50", "label": "Nifty Midcap 50", "type": "market"},
    }
    SECTOR_SYMBOLS = {
        "BANK":              {"key": "NSE_INDEX|Nifty Bank",              "label": "Nifty Bank"},
        "IT":                {"key": "NSE_INDEX|Nifty IT",                "label": "Nifty IT"},
        "FIN_SERVICES":      {"key": "NSE_INDEX|Nifty Fin Service",       "label": "Nifty Fin Services"},
        "AUTO":              {"key": "NSE_INDEX|Nifty Auto",              "label": "Nifty Auto"},
        "FMCG":              {"key": "NSE_INDEX|Nifty FMCG",              "label": "Nifty FMCG"},
        "METAL":             {"key": "NSE_INDEX|Nifty Metal",             "label": "Nifty Metal"},
        "PHARMA":            {"key": "NSE_INDEX|Nifty Pharma",            "label": "Nifty Pharma"},
        "REALTY":            {"key": "NSE_INDEX|Nifty Realty",            "label": "Nifty Realty"},
        "OIL_GAS":           {"key": "NSE_INDEX|Nifty Oil and Gas",       "label": "Nifty Oil & Gas"},
        "CONSUMER_DURABLES": {"key": "NSE_INDEX|Nifty Consr Durbl",       "label": "Nifty Consumer Durables"},
    }
    ALL_INDEX_KEYS.clear()
    for sid, meta in {**MARKET_SYMBOLS, **SECTOR_SYMBOLS}.items():
        ALL_INDEX_KEYS[meta["key"]] = sid


# ---------------------------------------------------------------------------
# 2. AUTHENTICATION — Upstox TOTP
# ---------------------------------------------------------------------------

def _patch_upstox_response_parsing():
    """
    Monkey-patch upstox-totp so missing fields in Upstox's API response
    don't crash Pydantic validation. Specifically: Upstox no longer
    returns a 'poa' field but the library's AccessTokenResponse model
    requires it. Token itself is valid; only the parser was broken.
    """
    try:
        from upstox_totp._api.app_token import AccessTokenResponse

        _orig = AccessTokenResponse.model_validate.__func__
        _defaults = {"poa": False}

        @classmethod
        def _tolerant_validate(cls, obj, *a, **kw):
            if isinstance(obj, dict):
                data = obj.get("data")
                if isinstance(data, dict):
                    for field, default in _defaults.items():
                        if field not in data:
                            data[field] = default
            return _orig(cls, obj, *a, **kw)

        AccessTokenResponse.model_validate = _tolerant_validate
        print("[AUTH] Patched AccessTokenResponse.model_validate (poa fix)")

    except Exception as e:
        print(f"[AUTH] Response parsing patch failed: {e}")


def get_access_token() -> str:
    from upstox_totp import UpstoxTOTP

    _patch_upstox_response_parsing()

    debug_mode = os.environ.get("UPSTOX_DEBUG", "false").lower() in ("true", "1")
    print(f"[AUTH] Initialising upstox-totp (debug={debug_mode}) ...")

    try:
        upx = UpstoxTOTP(debug=debug_mode)
        resp = upx.app_token.get_access_token()

        if resp.success and resp.data:
            print(f"[AUTH] Token obtained for user: {resp.data.user_id}")
            return resp.data.access_token
        else:
            error_msg = getattr(resp, "error", "unknown error")
            raise RuntimeError(f"Token generation failed: {error_msg}")

    except Exception as e:
        print(f"\n[AUTH] ✗ Authentication failed: {e}")
        print("[AUTH] Troubleshooting:")
        print("[AUTH]   1. Is UPSTOX_TOTP_SECRET the raw key (letters+digits)?")
        print("[AUTH]   2. Is UPSTOX_PIN_CODE your 6-digit login PIN?")
        print("[AUTH]   3. Do CLIENT_ID / SECRET / REDIRECT_URI match your app?")
        print("[AUTH]   4. Wait 30 min if you've hit attempt limits, then retry.")
        raise


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
                time.sleep(RETRY_DELAY * attempt)
            else:
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
        except requests.exceptions.RequestException:
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    return []


def fetch_all_index_data(access_token: str) -> dict[str, list[list]]:
    from_date = (datetime.now() - timedelta(days=INDEX_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

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
    s.get(NSE_BASE, timeout=15)
    time.sleep(0.5)
    return s


def fetch_nifty500_constituents() -> list[dict]:
    session = _nse_session()

    for index_name in ["NIFTY 500", "NIFTY 200", "NIFTY 100"]:
        try:
            print(f"  [BREADTH] Trying NSE API for {index_name} ...", end=" ", flush=True)
            r = session.get(NSE_INDEX_API, params={"index": index_name}, timeout=20)
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


def _fetch_stock_above_ema(args) -> tuple[str, bool | None]:
    symbol, upstox_key, access_token, from_date, to_date = args
    try:
        candles = fetch_candles(upstox_key, access_token, from_date, to_date)
        if len(candles) < 20:
            return (symbol, None)

        closes = [c[4] for c in candles]
        period = 20
        multiplier = 2 / (period + 1)
        ema = sum(closes[:period]) / period
        for c in closes[period:]:
            ema = (c - ema) * multiplier + ema

        return (symbol, closes[-1] > ema)
    except Exception:
        return (symbol, None)


def compute_market_breadth(constituents: list[dict], access_token: str) -> dict:
    from_date = (datetime.now() - timedelta(days=BREADTH_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")


    print(f"\n[BREADTH] Computing breadth for {len(constituents)} stocks "
          f"({from_date} to {to_date}, {BREADTH_WORKERS} threads)\n")

    tasks = [(c["symbol"], c["upstox_key"], access_token, from_date, to_date)
             for c in constituents]

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

            if done % 50 == 0 or done == total:
                computed = above_count + below_count
                pct = (above_count / computed * 100) if computed > 0 else 0
                print(f"  [BREADTH] {done:3d}/{total} fetched | "
                      f"Above: {above_count} | Below: {below_count} | "
                      f"Errors: {error_count} | Running: {pct:.1f}%")

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
# 5. TECHNICAL CALCULATIONS (indices)
# ---------------------------------------------------------------------------

def calc_ema(closes: list[float], period: int) -> list[float | None]:
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


def count_consecutive_days_below(closes: list[float], ema_arr: list) -> int:
    """
    Count consecutive trading days (before today) where close < EMA.
    Used to annotate 'Nifty reclaimed 20 EMA after N days below' in insights.
    Today is the last element; counting walks backwards from the second-last.
    """
    count = 0
    for i in range(len(closes) - 2, -1, -1):
        if i >= len(ema_arr) or ema_arr[i] is None:
            break
        if closes[i] < ema_arr[i]:
            count += 1
        else:
            break
    return count


def process_symbol(candles: list[list]) -> dict:
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
    close = nifty.get("current_close")
    ema20 = nifty.get("ema_20")
    ema50 = nifty.get("ema_50")
    ema200 = nifty.get("ema_200")
    vix_close = vix.get("current_close")

    if any(v is None for v in [close, ema20, ema50, ema200]):
        return {"regime": "Unknown", "active_modules": [],
                "description": "Insufficient data for classification", "colour": "gray"}

    if breadth_pct is not None:
        breadth = breadth_pct
        bsrc = "Nifty 500"
    else:
        total = len(sector_regimes)
        bullish = sum(1 for s in sector_regimes.values() if s.get("regime") == "Bullish")
        breadth = (bullish / total * 100) if total > 0 else 50
        bsrc = "sector proxy"

    vix_val = vix_close if vix_close is not None else 15

    if close < ema200 and vix_val > 22:
        return {"regime": "Bear", "active_modules": [],
                "description": f"Nifty below 200 EMA, VIX {vix_val:.1f}. Breadth {breadth:.0f}% ({bsrc})",
                "colour": "red"}

    if close < ema20 and close < ema50:
        return {"regime": "Correcting", "active_modules": ["M2"],
                "description": f"Nifty below 20 & 50 EMA. Breadth {breadth:.0f}% ({bsrc})",
                "colour": "orange"}

    if close < ema20 and close >= ema50:
        return {"regime": "Recovering", "active_modules": ["M2", "M3"],
                "description": f"Nifty below 20 EMA, above 50 EMA. Breadth {breadth:.0f}% ({bsrc})",
                "colour": "yellow"}

    pct_from_ema20 = abs(close - ema20) / ema20 * 100

    if pct_from_ema20 <= 1.0 and 40 <= breadth <= 60:
        return {"regime": "Sideways", "active_modules": ["M3"],
                "description": f"Nifty within 1% of 20 EMA. Breadth {breadth:.0f}% ({bsrc})",
                "colour": "yellow"}

    if close > ema20 and close > ema50 and breadth > 60 and vix_val < 16:
        return {"regime": "Bullish Trending", "active_modules": ["M1", "M3"],
                "description": f"Nifty above 20 & 50 EMA, VIX {vix_val:.1f}, breadth {breadth:.0f}% ({bsrc})",
                "colour": "green"}

    if close > ema20 and close > ema50:
        notes = []
        if vix_val >= 16:
            notes.append(f"VIX elevated ({vix_val:.1f})")
        if breadth <= 60:
            notes.append(f"breadth moderate ({breadth:.0f}%)")
        detail = "; ".join(notes) if notes else "all clear"
        return {"regime": "Bullish Trending", "active_modules": ["M1", "M3"],
                "description": f"Nifty above 20 & 50 EMA. {detail}. ({bsrc})",
                "colour": "green"}

    return {"regime": "Sideways", "active_modules": ["M3"],
            "description": f"Mixed signals. Breadth {breadth:.0f}% ({bsrc})",
            "colour": "yellow"}


def classify_sector_regime(data: dict) -> dict:
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
# 7. DECISION LAYER — transition, insight, playbook, scan here
# ---------------------------------------------------------------------------

def _load_json_history(path: str) -> list[dict]:
    p = Path(path)
    if not p.exists():
        return []
    try:
        data = json.loads(p.read_text())
        return data if isinstance(data, list) else []
    except Exception:
        return []


def append_regime_history(path: str, iso_date: str, regime: str):
    p = Path(path)
    hist = _load_json_history(path)
    hist = [h for h in hist if h.get("date") != iso_date]
    hist.append({"date": iso_date, "regime": regime})
    hist.sort(key=lambda h: h["date"])
    hist = hist[-HISTORY_KEEP:]
    p.write_text(json.dumps(hist, indent=2))


def append_breadth_history(path: str, iso_date: str, breadth_pct: float | None):
    if breadth_pct is None:
        return
    p = Path(path)
    hist = _load_json_history(path)
    hist = [h for h in hist if h.get("date") != iso_date]
    hist.append({"date": iso_date, "breadth_pct": round(float(breadth_pct), 2)})
    hist.sort(key=lambda h: h["date"])
    hist = hist[-HISTORY_KEEP:]
    p.write_text(json.dumps(hist, indent=2))


def compute_regime_transition(history: list[dict], current_regime: str,
                              today_iso: str) -> dict:
    """
    Build the 5-day transition strip and status label.
    `history` should NOT yet contain today's entry; today is appended here.
    """
    working = [h for h in history if h.get("date") != today_iso]
    working.append({"date": today_iso, "regime": current_regime})
    last5 = working[-5:]

    strip = [{
        "date":   h["date"],
        "regime": h["regime"],
        "label":  REGIME_LABEL.get(h["regime"], "?"),
        "colour": REGIME_COLOUR.get(h["regime"], "side"),
    } for h in last5]

    # Days in current regime (consecutive, ending today)
    days_in = 1
    for h in reversed(last5[:-1]):
        if h["regime"] == current_regime:
            days_in += 1
        else:
            break

    regimes = [h["regime"] for h in last5]
    flips = sum(1 for i in range(1, len(regimes)) if regimes[i] != regimes[i-1])

    if flips >= 3:
        status, direction = "CHOPPY", "choppy"
        note = "3+ flips in 5 days — stand aside today"
    else:
        ranks = [REGIME_RANK.get(r, 2) for r in regimes]
        delta = ranks[-1] - ranks[0] if len(ranks) >= 2 else 0

        if days_in == 1 and current_regime == "Bullish Trending":
            status, direction = "FRESH BULL", "improving"
            note = "Day 1 confirming — size half, do not chase"
        elif delta >= 2:
            status, direction = "IMPROVING", "improving"
            note = "Regime climbing — re-engage cautiously"
        elif delta <= -2:
            status, direction = "WEAKENING", "weakening"
            note = "Regime deteriorating — tighten and reduce"
        elif days_in >= 3:
            status = f"STABLE {REGIME_LABEL.get(current_regime, '')}".strip()
            direction = "stable"
            note = "Trend intact — normal sizing"
        else:
            status = REGIME_LABEL.get(current_regime, current_regime)
            direction = "stable"
            note = "Regime unchanged"

    return {
        "history": strip,
        "status": status,
        "days_in_regime": days_in,
        "direction": direction,
        "note": note,
    }


def compute_market_insight(
    nifty: dict,
    vix: dict,
    nifty_candles: list[list],
    breadth_pct: float | None,
    breadth_history: list[dict],
) -> list[dict]:
    """
    Rules-only insights. Each line is triggered by a specific numeric
    threshold crossing. No prose, no opinion. Max 3 lines, ranked by severity.
    """
    out = []

    close = nifty.get("current_close")
    prev = nifty.get("prev_close")
    ema20_last = nifty.get("ema_20")
    ema50_last = nifty.get("ema_50")

    # --- Nifty 20 EMA reclaim / lose ---
    if close is not None and prev is not None and ema20_last is not None and nifty_candles:
        closes = [c[4] for c in nifty_candles]
        ema20_arr = calc_ema(closes, 20)

        # Yesterday's EMA20 value (for crossover detection)
        prev_ema20 = ema20_arr[-2] if len(ema20_arr) >= 2 and ema20_arr[-2] is not None else None

        if prev_ema20 is not None:
            if close >= ema20_last and prev < prev_ema20:
                days_below = count_consecutive_days_below(closes, ema20_arr)
                suffix = f" after {days_below} day{'s' if days_below != 1 else ''} below" if days_below else ""
                out.append({"severity": "high", "trigger": "nifty_reclaim_20ema",
                            "text": f"Nifty reclaimed 20 EMA{suffix} — M1 re-activated"})
            elif close < ema20_last and prev >= prev_ema20:
                out.append({"severity": "high", "trigger": "nifty_lose_20ema",
                            "text": "Nifty lost 20 EMA — M1 setups invalidated"})

        # --- Nifty 50 EMA cross ---
        ema50_arr = calc_ema(closes, 50)
        prev_ema50 = ema50_arr[-2] if len(ema50_arr) >= 2 and ema50_arr[-2] is not None else None

        if prev_ema50 is not None and ema50_last is not None:
            if close >= ema50_last and prev < prev_ema50:
                out.append({"severity": "medium", "trigger": "nifty_reclaim_50ema",
                            "text": "Nifty reclaimed 50 EMA — medium-term trend confirming"})
            elif close < ema50_last and prev >= prev_ema50:
                out.append({"severity": "high", "trigger": "nifty_lose_50ema",
                            "text": "Nifty lost 50 EMA — medium-term trend at risk"})

    # --- Breadth shift (3d) ---
    if breadth_pct is not None and breadth_history:
        # breadth_history excludes today. 3 trading days ago = index -3
        if len(breadth_history) >= 3:
            b3 = breadth_history[-3].get("breadth_pct")
            if b3 is not None:
                d = breadth_pct - b3
                if d >= 10:
                    out.append({"severity": "high", "trigger": "breadth_expansion",
                                "text": f"Breadth {b3:.0f}% → {breadth_pct:.0f}% in 3 days — participation broadening, M3 odds up"})
                elif d <= -10:
                    out.append({"severity": "high", "trigger": "breadth_contraction",
                                "text": f"Breadth {b3:.0f}% → {breadth_pct:.0f}% in 3 days — narrow leadership, tighten"})

    # --- VIX regime ---
    vc = vix.get("current_close")
    vp = vix.get("prev_close")
    if vc is not None and vp is not None:
        if vc >= 16 and vp < 16:
            out.append({"severity": "high", "trigger": "vix_high",
                        "text": "VIX > 16 — volatility regime, widen stops or skip new entries"})
        elif vc < 14 and vp >= 14:
            out.append({"severity": "medium", "trigger": "vix_low",
                        "text": "VIX < 14 — complacency zone, watch for volatility reversal"})
        elif vc < 14:
            out.append({"severity": "low", "trigger": "vix_low_persist",
                        "text": "VIX < 14 — complacency zone, watch for volatility reversal"})

    rank = {"high": 0, "medium": 1, "low": 2}
    out.sort(key=lambda x: rank.get(x["severity"], 9))
    return out[:3]


def compute_playbook(
    regime: str,
    sector_regimes_full: dict,
    transition: dict,
    open_trades: int = 0,
    max_open: int = 5,
) -> dict:
    """
    Build today's playbook. Collapses to stand-aside on CHOPPY regime.
    Halves size on FRESH BULL and WEAKENING transitions.
    """
    run = [{"module": m, "priority": p, "name": n}
           for (m, p, n) in MODULE_MAP.get(regime, [])]
    skip = [{"module": m, "name": n} for (m, n) in SKIP_MAP.get(regime, [])]

    focus, watch, avoid = [], [], []
    for code, s in sector_regimes_full.items():
        reg = s.get("regime", "")
        if reg == "Bullish":
            focus.append(code)
        elif reg in ("Recovering", "Sideways"):
            watch.append(code)
        elif reg == "Correcting":
            avoid.append(code)

    cap = max(0, max_open - open_trades)
    cap = min(cap, 2)

    size_rule = "full"
    size_reason = ""
    override = None

    if transition.get("direction") == "choppy":
        override = "CHOPPY regime — stand aside today, no new entries"
        run = []
        cap = 0
    elif transition.get("status") == "FRESH BULL":
        size_rule = "half"
        size_reason = "FRESH BULL day 1 — confirm before full size"
    elif transition.get("direction") == "weakening":
        size_rule = "half"
        size_reason = "Regime weakening — reduce exposure"

    return {
        "run": run,
        "skip": skip,
        "focus_sectors": focus,
        "watch_sectors": watch,
        "avoid_sectors": avoid,
        "cap_new_entries": cap,
        "size_rule": size_rule,
        "size_reason": size_reason,
        "override": override,
    }


def compute_scan_here(sector_regimes_full: dict) -> dict:
    """
    Bucket sectors by their distance from 20 EMA into actionable zones:

      READY    (0% to +3% above 20 EMA)  → M1/M3 scan here today
      EXTENDED (>+3% above 20 EMA)        → wait for pullback, do NOT chase
      WEAK     (below 20 EMA)             → M2 candidates only if regime is
                                            Recovering / Correcting

    Replaces the previous top-3/bottom-3 ranking which pointed traders at
    the most extended sectors for M1 — impossible setups, since M1 requires
    a pullback to 20 EMA.

    Within each bucket:
      READY    sorted closest-to-20-EMA first (cleanest M1 targets)
      EXTENDED sorted most-extended first (biggest warnings first)
      WEAK     sorted most-oversold first (best M2 candidates first)
    """
    ready, extended, weak = [], [], []

    for code, s in sector_regimes_full.items():
        close = s.get("current_close")
        ema20 = s.get("ema_20")
        if not (close and ema20 and ema20 > 0):
            continue
        dist = (close - ema20) / ema20 * 100.0
        item = {"sector": code, "distance_pct": round(dist, 2)}

        if dist < 0:
            weak.append(item)
        elif dist <= SCAN_READY_MAX:
            ready.append(item)
        else:
            extended.append(item)

    ready.sort(key=lambda x: x["distance_pct"])                 # 0% first
    extended.sort(key=lambda x: x["distance_pct"], reverse=True) # most extended first
    weak.sort(key=lambda x: x["distance_pct"])                  # most negative first

    return {
        "ready": ready,
        "extended": extended,
        "weak": weak,
        "ready_threshold_pct": SCAN_READY_MAX,
    }


def compute_today_mode(transition: dict) -> dict:
    """
    Map transition status → concrete 3-line action guidance.
    The trader's single most important morning read.
    """
    status = transition.get("status", "")
    mode, wait_for, dont = TODAY_MODE_MAP.get(status, TODAY_MODE_DEFAULT)
    return {"mode": mode, "wait_for": wait_for, "dont": dont}


def compute_insight_fallback(transition: dict) -> str:
    """Regime-appropriate one-liner used when no insight rules fire."""
    return INSIGHT_FALLBACK_MAP.get(transition.get("status", ""),
                                     "Regime unchanged — follow the playbook.")


def detect_data_status(nifty_candles: list[list]) -> dict:
    """
    Determine what the data in this JSON actually represents.
    Returns data_as_of date and whether the most recent candle is intraday-partial.

    Rule:
      If last candle date == today (IST) AND current IST time < 15:30 on a
      weekday → intraday partial (today's candle not yet closed).
      Otherwise → EOD final.

    This is the authoritative answer to "is this today's close or not?".
    """
    now = datetime.now()

    if not nifty_candles:
        return {
            "data_as_of": None,
            "data_as_of_label": "— unavailable —",
            "is_intraday_partial": False,
            "status_text": "✗ NO DATA",
        }

    # Upstox candle format: [timestamp, O, H, L, C, volume, OI]
    # Timestamp may be 'YYYY-MM-DDTHH:MM:SS+05:30' or similar ISO string.
    last_ts = str(nifty_candles[-1][0])
    last_date_str = last_ts[:10]  # YYYY-MM-DD
    try:
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").date()
    except ValueError:
        return {
            "data_as_of": None,
            "data_as_of_label": "— unavailable —",
            "is_intraday_partial": False,
            "status_text": "✗ NO DATA",
        }

    today = now.date()
    is_weekday = today.weekday() < 5                 # Mon–Fri
    current_minutes = now.hour * 60 + now.minute
    market_close_minutes = 15 * 60 + 30              # 15:30 IST

    is_intraday_partial = (
        last_date == today
        and is_weekday
        and current_minutes < market_close_minutes
    )

    data_as_of_label = last_date.strftime("%a %d %b %Y") + " close"
    if is_intraday_partial:
        status_text = "⚠ INTRADAY — today's candle not yet closed"
    elif last_date == today:
        status_text = "✓ EOD final (today's close)"
    else:
        status_text = "✓ EOD final"

    return {
        "data_as_of": last_date.strftime("%Y-%m-%d"),
        "data_as_of_label": data_as_of_label,
        "is_intraday_partial": is_intraday_partial,
        "status_text": status_text,
    }


# ---------------------------------------------------------------------------
# 8. JSON OUTPUT
# ---------------------------------------------------------------------------

def build_dashboard_json(
    raw_index_data: dict[str, list[list]],
    breadth_data: dict | None,
    regime_history: list[dict],
    breadth_history: list[dict],
    today_iso: str,
) -> dict:
    now = datetime.now()

    # --- Process market symbols ---
    market_processed = {}
    for sid, meta in MARKET_SYMBOLS.items():
        candles = raw_index_data.get(meta["key"], [])
        processed = process_symbol(candles)
        market_processed[sid] = {**processed, "label": meta["label"],
                                 "instrument_key": meta["key"]}

    # --- Process sector symbols ---
    sector_processed = {}
    sector_regimes_classification = {}
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
            "regime_colour": REGIME_COLOUR.get(regime["regime"], "side"),
        }
        sector_regimes_classification[sid] = regime

    # --- Market regime ---
    nifty_data = market_processed.get("NIFTY_50", {})
    vix_data = market_processed.get("INDIA_VIX", {})
    real_breadth = breadth_data.get("breadth_pct") if breadth_data else None
    market_regime = classify_market_regime(
        nifty_data, vix_data, real_breadth, sector_regimes_classification
    )

    # --- DECISION LAYER ---
    transition = compute_regime_transition(
        regime_history, market_regime["regime"], today_iso
    )

    nifty_key = MARKET_SYMBOLS.get("NIFTY_50", {}).get("key", "")
    nifty_candles = raw_index_data.get(nifty_key, [])

    # Data status: is this EOD final or intraday partial?
    data_status = detect_data_status(nifty_candles)

    insight = compute_market_insight(
        nifty_data, vix_data, nifty_candles, real_breadth, breadth_history
    )
    insight_fallback = compute_insight_fallback(transition)

    playbook = compute_playbook(
        market_regime["regime"], sector_processed, transition
    )
    # Today's Mode — lives inside the playbook block so the HTML can render it
    # as the first thing inside the Playbook card.
    playbook["today_mode"] = compute_today_mode(transition)

    scan_here = compute_scan_here(sector_processed)

    # --- Enriched breadth block with 3d delta ---
    breadth_block = dict(breadth_data) if breadth_data else {
        "breadth_pct": None, "above_ema": None, "below_ema": None,
        "errors": None, "total_constituents": None,
        "total_computed": None, "index_used": "unavailable",
    }
    if real_breadth is not None and len(breadth_history) >= 3:
        b3 = breadth_history[-3].get("breadth_pct")
        if b3 is not None:
            breadth_block["breadth_3d_ago"] = b3
            breadth_block["change_3d"] = round(real_breadth - b3, 2)

    # --- Assemble output ---
    return {
        "meta": {
            "generated_at": now.isoformat(),
            "generated_date": now.strftime("%Y-%m-%d"),
            "generated_time": now.strftime("%H:%M IST"),
            "data_source": "Upstox API v2",
            "index_lookback_days": INDEX_LOOKBACK_DAYS,
            "breadth_lookback_days": BREADTH_LOOKBACK_DAYS,
            "schema_version": "1.3",
            # Data status — what this JSON actually represents
            "data_as_of":          data_status["data_as_of"],
            "data_as_of_label":    data_status["data_as_of_label"],
            "is_intraday_partial": data_status["is_intraday_partial"],
            "data_status_text":    data_status["status_text"],
        },
        "market_pulse": {
            "nifty":      market_processed.get("NIFTY_50"),
            "bank_nifty": market_processed.get("BANK_NIFTY"),
            "midcap":     market_processed.get("MIDCAP"),
            "vix":        market_processed.get("INDIA_VIX"),
        },
        "market_regime": market_regime,
        "regime_transition": transition,
        "breadth": breadth_block,
        "market_insight": insight,
        "insight_fallback_text": insight_fallback,
        "playbook": playbook,
        "scan_here": scan_here,
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
# 9. MAIN
# ---------------------------------------------------------------------------

def main():
    start_time = time.time()

    print("=" * 65)
    print("  Swing Trading Dashboard — Data Fetch v1.2")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 65)

    # -- Step 1: Authenticate --
    print("\n[STEP 1/6] Authenticating with Upstox ...")
    try:
        access_token = get_access_token()
    except Exception as e:
        print(f"\n[FATAL] Authentication failed: {e}")
        traceback.print_exc()
        sys.exit(1)

    # -- Step 2: Resolve instrument keys --
    print("\n[STEP 2/6] Resolving instrument keys from Upstox ...")
    resolve_instrument_keys()

    if "NIFTY_50" not in MARKET_SYMBOLS:
        print("\n[FATAL] Could not resolve Nifty 50 instrument key")
        sys.exit(1)

    # -- Step 3: Fetch index data --
    print("\n[STEP 3/6] Fetching index historical data ...")
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

    # -- Step 4: Fetch Nifty 500 constituents --
    print("\n[STEP 4/6] Fetching Nifty 500 constituent list from NSE ...")
    try:
        constituents = fetch_nifty500_constituents()
    except Exception as e:
        print(f"  [BREADTH] Failed to get constituents: {e}")
        constituents = []

    # -- Step 5: Compute breadth --
    breadth_data = None
    if constituents:
        print(f"\n[STEP 5/6] Computing market breadth ({len(constituents)} stocks) ...")
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
        print("\n[STEP 5/6] Skipping breadth (no constituent list) — will use sector proxy")

    # -- Step 6: Decision layer + JSON output --
    print("\n[STEP 6/6] Building decision layer and writing JSON ...")

    output_path         = os.environ.get("OUTPUT_PATH",          "dashboard_data.json")
    regime_history_path = os.environ.get("REGIME_HISTORY_PATH",  "regime_history.json")
    breadth_history_path= os.environ.get("BREADTH_HISTORY_PATH", "breadth_history.json")

    # Load histories BEFORE computing decision layer — today must not yet be in them
    regime_history  = _load_json_history(regime_history_path)
    breadth_history = _load_json_history(breadth_history_path)

    today_iso = datetime.now().strftime("%Y-%m-%d")

    dashboard = build_dashboard_json(
        raw_index_data, breadth_data,
        regime_history, breadth_history, today_iso,
    )

    with open(output_path, "w") as f:
        json.dump(dashboard, f, indent=2, ensure_ascii=False)

    # Append today's values to history files (AFTER computing insight/transition,
    # so tomorrow's run sees today as a prior data point)
    append_regime_history(regime_history_path, today_iso, dashboard["market_regime"]["regime"])
    if breadth_data and breadth_data.get("breadth_pct") is not None:
        append_breadth_history(breadth_history_path, today_iso, breadth_data["breadth_pct"])

    # -- Summary --
    elapsed = time.time() - start_time
    meta = dashboard["meta"]
    regime = dashboard["market_regime"]["regime"]
    trans = dashboard["regime_transition"]
    pb = dashboard["playbook"]
    tm = pb.get("today_mode", {})
    sh = dashboard["scan_here"]
    modules = ", ".join(m["module"] for m in pb["run"]) or "— stand aside —"
    nifty_close = dashboard["market_pulse"]["nifty"]["current_close"]
    vix_close = dashboard["market_pulse"]["vix"]["current_close"]
    breadth_val = dashboard["breadth"]["breadth_pct"]

    def _fmt_scan(items):
        return ", ".join(f"{i['sector']} ({i['distance_pct']:+.1f}%)" for i in items) or "—"

    print(f"\n{'=' * 65}")
    print(f"  Done in {elapsed:.1f}s  |  Output: {output_path}")
    print(f"")
    print(f"  DATA AS OF      {meta['data_as_of_label']}")
    print(f"  UPDATED         {meta['generated_date']} · {meta['generated_time']}")
    print(f"  STATUS          {meta['data_status_text']}")
    print(f"")
    print(f"  Nifty 50:       {nifty_close}")
    print(f"  India VIX:      {vix_close}")
    if breadth_val is not None:
        print(f"  Breadth:        {breadth_val}% of Nifty 500 above 20 EMA")
    else:
        print(f"  Breadth:        unavailable (using sector proxy)")
    print(f"  Regime:         {regime}")
    print(f"  Transition:     {trans['status']}  ({trans['direction']}, day {trans['days_in_regime']})")
    print(f"")
    print(f"  ── Today's Mode ──")
    print(f"    MODE       {tm.get('mode', '—')}")
    print(f"    WAIT FOR   {tm.get('wait_for', '—')}")
    print(f"    DON'T      {tm.get('dont', '—')}")
    print(f"")
    print(f"  Playbook RUN:   {modules}")
    if pb['override']:
        print(f"  OVERRIDE:       {pb['override']}")
    print(f"  Focus sectors:  {', '.join(pb['focus_sectors']) or '—'}")
    print(f"  Avoid sectors:  {', '.join(pb['avoid_sectors']) or '—'}")
    print(f"")
    print(f"  Scan Here:")
    print(f"    READY       {_fmt_scan(sh['ready'])}")
    print(f"    EXTENDED    {_fmt_scan(sh['extended'])}    (wait for pullback)")
    print(f"    WEAK        {_fmt_scan(sh['weak'])}        (M2 only)")
    if dashboard['market_insight']:
        print(f"")
        print(f"  Insight:")
        for i in dashboard['market_insight']:
            print(f"    → {i['text']}")
    else:
        print(f"")
        print(f"  Insight:        → {dashboard['insight_fallback_text']}")
    print(f"")
    print(f"  Sector Regimes:")
    for sid, sdata in dashboard["sector_regimes"].items():
        print(f"    {sdata['label']:30s}  {sdata['regime']:12s}  {sdata['position_size_rule']}")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
