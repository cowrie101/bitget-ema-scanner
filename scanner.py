import os
import time
import json
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import pandas as pd
import websocket


# ============================================================
# BITGET EMA20 / EMA50 REAL-TIME WEBSOCKET SCANNER
#
# SPOT USDT PAIRS
# 30-MINUTE CANDLES
#
# FEATURES
# ------------------------------------------------------------
# EMA 20
# EMA 50
# Actual Golden Cross
# Actual Death Cross
# Impending Golden Cross
# Impending Death Cross
# Forming candle
# Market condition
# Trend condition
# Market structure
# Telegram alerts
#
# REMOVED
# ------------------------------------------------------------
# ADX
# +DI / -DI
# Volume
# Volume ratio
# Bollinger Bands
# RSI
# Squeeze
#
# SIDEWAYS MARKETS ARE EXCLUDED
# ============================================================


# ============================================================
# BITGET REST API
# ============================================================

BITGET_SYMBOLS = (
    "https://api.bitget.com/api/v2/spot/public/symbols"
)

BITGET_CANDLES = (
    "https://api.bitget.com/api/v2/spot/market/history-candles"
)


# ============================================================
# BITGET WEBSOCKET
# ============================================================

BITGET_WS_URL = (
    "wss://ws.bitget.com/v2/ws/public"
)


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_SEND = (
    "https://api.telegram.org/bot{}/sendMessage"
)


# ============================================================
# SETTINGS
# ============================================================

EMA_FAST = 20
EMA_SLOW = 50

GRANULARITY = "30min"

CANDLE_LIMIT = 200


# ============================================================
# IMPENDING CROSS
#
# Example:
#
# EMA20 is still below EMA50 but very close
# AND EMA20 is moving upward.
#
# This produces:
#
# IMPENDING GOLDEN CROSS
#
# Same idea in reverse for death cross.
# ============================================================

IMPENDING_CROSS_PERCENT = 0.15


# ============================================================
# TREND SETTINGS
# ============================================================

# Number of candles used to measure EMA movement.
TREND_LOOKBACK = 5


# Minimum EMA50 movement required to consider
# the market directional rather than flat.
#
# This is intentionally modest because crypto pairs
# can have very different price behaviour.
MIN_EMA50_SLOPE_PERCENT = 0.03


# Minimum EMA20 movement required to confirm
# directional momentum.
MIN_EMA20_SLOPE_PERCENT = 0.03


# ============================================================
# MARKET STRUCTURE
# ============================================================

STRUCTURE_LOOKBACK = 5


# ============================================================
# WEBSOCKET SETTINGS
# ============================================================

# Bitget allows multiple subscriptions per connection.
# 40 keeps the connection comfortably sized.
WS_PAIRS_PER_CONNECTION = 40

WS_PING_INTERVAL = 20

WS_RECONNECT_DELAY = 5


# ============================================================
# REST SETTINGS
# ============================================================

REQUEST_TIMEOUT = 10

MAX_RETRIES = 3

MAX_WORKERS = 12

REQUESTS_PER_SECOND = 19


# ============================================================
# TELEGRAM SECRETS
# ============================================================

TELEGRAM_BOT_TOKEN = os.environ.get(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.environ.get(
    "TELEGRAM_CHAT_ID"
)


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Bitget-EMA20-EMA50-WebSocket-Scanner/5.0"
})


# ============================================================
# THREAD-LOCAL HTTP SESSIONS
# ============================================================

thread_local = threading.local()


def get_thread_session():

    if not hasattr(
        thread_local,
        "session"
    ):

        thread_local.session = requests.Session()

        thread_local.session.headers.update({
            "User-Agent":
                "Bitget-EMA20-EMA50-WebSocket-Scanner/5.0"
        })

    return thread_local.session


# ============================================================
# RATE LIMITER
# ============================================================

rate_lock = threading.Lock()

request_times = deque()


def wait_for_request_slot():

    while True:

        now = time.monotonic()

        with rate_lock:

            while (
                request_times
                and now - request_times[0] >= 1.0
            ):

                request_times.popleft()

            if len(request_times) < REQUESTS_PER_SECOND:

                request_times.append(now)

                return

            wait_time = (
                1.0
                - (now - request_times[0])
                + 0.005
            )

        if wait_time > 0:

            time.sleep(wait_time)


# ============================================================
# SHARED CANDLE STORAGE
#
# candles[symbol] = deque of historical + live candles
# ============================================================

candles = {}

candles_lock = threading.Lock()


# ============================================================
# WEBSOCKET STATUS
# ============================================================

ws_updates = 0

ws_data_messages = 0

ws_candle_messages = 0

ws_connections_live = 0

ws_lock = threading.Lock()


# ============================================================
# LAST ALERT STATE
#
# Prevents repeated Telegram alerts for the same setup.
# ============================================================

last_alert_state = {}

alert_lock = threading.Lock()


# ============================================================
# GET ALL ONLINE USDT SPOT PAIRS
# ============================================================

def get_symbols():

    print("Getting Bitget Spot USDT symbols...")

    response = session.get(
        BITGET_SYMBOLS,
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    if data.get("code") != "00000":

        raise RuntimeError(
            f"Bitget symbols error: {data}"
        )

    symbols = []

    for item in data.get("data", []):

        symbol = item.get(
            "symbol",
            ""
        )

        quote_coin = item.get(
            "quoteCoin",
            ""
        )

        status = item.get(
            "status",
            ""
        )

        if (
            quote_coin == "USDT"
            and status == "online"
            and symbol.endswith("USDT")
        ):

            symbols.append(symbol)

    symbols = sorted(
        set(symbols)
    )

    print(
        f"Found {len(symbols)} online "
        f"USDT Spot pairs."
    )

    return symbols


# ============================================================
# GET HISTORICAL CANDLES
# ============================================================

def get_historical_candles(symbol):

    end_time = int(
        time.time() * 1000
    )

    params = {
        "symbol": symbol,
        "granularity": GRANULARITY,
        "endTime": str(end_time),
        "limit": str(CANDLE_LIMIT)
    }

    worker_session = get_thread_session()

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            wait_for_request_slot()

            response = worker_session.get(
                BITGET_CANDLES,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code in (
                429,
                500,
                502,
                503,
                504
            ):

                if attempt < MAX_RETRIES - 1:

                    time.sleep(
                        0.5 * (attempt + 1)
                    )

                    continue

                return None

            if response.status_code == 400:

                return None

            response.raise_for_status()

            data = response.json()

            if data.get("code") != "00000":

                return None

            rows = data.get(
                "data",
                []
            )

            if len(rows) < EMA_SLOW + 10:

                return None

            return rows

        except (
            requests.RequestException,
            ValueError
        ) as error:

            if attempt == MAX_RETRIES - 1:

                print(
                    f"{symbol}: historical request failed: "
                    f"{error}"
                )

                return None

            time.sleep(
                0.5 * (attempt + 1)
            )

    return None


# ============================================================
# CREATE DATAFRAME
# ============================================================

def make_dataframe(rows):

    cleaned_rows = []

    for row in rows:

        if len(row) >= 7:

            cleaned_rows.append(
                row[:7]
            )

    if not cleaned_rows:

        return pd.DataFrame()

    cleaned_rows = sorted(
        cleaned_rows,
        key=lambda x: int(x[0])
    )

    df = pd.DataFrame(
        cleaned_rows,
        columns=[
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume"
        ]
    )

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume"
    ]

    for column in numeric_columns:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df["ts"] = pd.to_numeric(
        df["ts"],
        errors="coerce"
    )

    df = df.dropna(
        subset=[
            "ts",
            "open",
            "high",
            "low",
            "close"
        ]
    )

    df = df.drop_duplicates(
        subset=["ts"],
