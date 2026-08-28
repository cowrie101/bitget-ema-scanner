import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import pandas as pd


# ============================================================
# BITGET EMA / ADX / MARKET STRUCTURE / VOLUME SCANNER
#
# FAST CONCURRENT VERSION
# ============================================================

BITGET_SYMBOLS = (
    "https://api.bitget.com/api/v2/spot/public/symbols"
)

BITGET_CANDLES = (
    "https://api.bitget.com/api/v2/spot/market/history-candles"
)

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

# ------------------------------------------------------------
# SCAN INTERVAL
#
# The scanner attempts to begin another scan every 30 seconds.
#
# IMPORTANT:
# If a full market scan takes longer than 30 seconds,
# the next scan starts immediately after the current scan.
# It will NEVER create overlapping scans.
# ------------------------------------------------------------

SCAN_INTERVAL = 30

# ------------------------------------------------------------
# CONCURRENT WORKERS
#
# Bitget documents the public candle endpoint at 20 requests/sec.
# We deliberately stay below that.
# ------------------------------------------------------------

MAX_WORKERS = 12

# Maximum candle-request starts per rolling second.
REQUESTS_PER_SECOND = 19

# HTTP timeout for individual requests.
REQUEST_TIMEOUT = 10

# Retry count for temporary API/network problems.
MAX_RETRIES = 3


# ============================================================
# IMPENDING CROSS
# ============================================================

IMPENDING_CROSS_PERCENT = 0.15


# ============================================================
# ADX SETTINGS
# ============================================================

ADX_PERIOD = 14


# ============================================================
# VOLUME SETTINGS
# ============================================================

VOLUME_LOOKBACK = 20


# ============================================================
# MARKET STRUCTURE SETTINGS
# ============================================================

STRUCTURE_LOOKBACK = 5


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
# MAIN SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Bitget-EMA-ADX-Scanner/4.0"
})


# ============================================================
# THREAD-LOCAL HTTP SESSIONS
#
# Each worker gets its own requests.Session.
# This avoids sharing one Session object between threads.
# ============================================================

thread_local = threading.local()


def get_thread_session():

    if not hasattr(
        thread_local,
        "session"
    ):

        thread_local.session = requests.Session()

        thread_local.session.headers.update({
            "User-Agent": "Bitget-EMA-ADX-Scanner/4.0"
        })

    return thread_local.session


# ============================================================
# REQUEST RATE LIMITER
#
# Keeps candle requests below Bitget's documented
# public endpoint rate limit.
# ============================================================

rate_lock = threading.Lock()

request_times = deque()


def wait_for_request_slot():

    while True:

        now = time.monotonic()

        with rate_lock:

            # Remove requests older than one second.
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
# MEMORY
#
# Prevents duplicate alerts.
# ============================================================

last_alert_state = {}

alert_lock = threading.Lock()


# ============================================================
# GET ALL ONLINE USDT SPOT PAIRS
# ============================================================

def get_symbols():

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

    return sorted(set(symbols))


# ============================================================
# GET 30-MINUTE CANDLES
# ============================================================

def get_candles(symbol):

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

            # ------------------------------------------------
            # Rate-limit every request attempt.
            # ------------------------------------------------

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
                    f"{symbol}: request failed: "
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
            "close",
            "volume"
        ]
    )

    return df


# ============================================================
# CALCULATE ADX
# ============================================================

def calculate_adx(
    df,
    period=14
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    previous_close = close.shift(1)

    tr1 = high - low

    tr2 = (
        high - previous_close
    ).abs()

    tr3 = (
        low - previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    up_move = (
        high - high.shift(1)
    )

    down_move = (
        low.shift(1) - low
    )

    plus_dm = up_move.where(
        (up_move > down_move)
        & (up_move > 0),
        0.0
    )

    minus_dm = down_move.where(
        (down_move > up_move)
        & (down_move > 0),
        0.0
    )

    atr = true_range.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    plus_di = (
        100
        * plus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr
    )

    minus_di = (
        100
        * minus_dm.ewm(
            alpha=1 / period,
            adjust=False
        ).mean()
        / atr
    )

    denominator = (
        plus_di + minus_di
    )

    dx = (
        100
        * (plus_di - minus_di).abs()
        / denominator
    )

    adx = dx.ewm(
        alpha=1 / period,
        adjust=False
    ).mean()

    return (
        pd.to_numeric(
            adx,
            errors="coerce"
        ).fillna(0),

        pd.to_numeric(
            plus_di,
            errors="coerce"
        ).fillna(0),

        pd.to_numeric(
            minus_di,
            errors="coerce"
        ).fillna(0)
    )


# ============================================================
# MARKET STRUCTURE
# ============================================================

def get_market_structure(df):

    lookback = STRUCTURE_LOOKBACK

    if len(df) < (
        lookback * 2 + 2
    ):

        return "NEUTRAL"

    recent = df.iloc[
        -lookback:
    ]

    previous = df.iloc[
        -(lookback * 2):-lookback
    ]

    recent_high = (
        recent["high"].max()
    )

    previous_high = (
        previous["high"].max()
    )

    recent_low = (
        recent["low"].min()
    )

    previous_low = (
        previous["low"].min()
    )

    if (
        recent_high > previous_high
        and recent_low > previous_low
    ):

        return "BULLISH"

    if (
        recent_high < previous_high
        and recent_low < previous_low
    ):

        return "BEARISH"

    return "NEUTRAL"


# ============================================================
# VOLUME ANALYSIS
# ============================================================

def get_volume_status(df):

    if len(df) < (
        VOLUME_LOOKBACK + 1
    ):

        return "NORMAL", 0.0

    current_volume = float(
        df.iloc[-1]["volume"]
    )

    average_volume = float(
        df["volume"]
        .iloc[
            -VOLUME_LOOKBACK - 1:-1
        ]
        .mean()
    )

    if average_volume <= 0:

        return "NORMAL", 0.0

    ratio = (
        current_volume
        / average_volume
    )

    if ratio >= 1.5:

        status = "HIGH"

    elif ratio >= 1.15:

        status = "ABOVE AVERAGE"

    elif ratio <= 0.70:

        status = "LOW"

    else:

        status = "NORMAL"

    return status, ratio


# ============================================================
# CHECK CROSS / IMPENDING CROSS
# ============================================================

def check_setup(symbol):

    rows = get_candles(
        symbol
    )

    if not rows:

        return None

    df = make_dataframe(
        rows
    )

    if df.empty:

        return None

    if len(df) < (
        EMA_SLOW + 20
    ):

        return None

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    df["ema20"] = df[
        "close"
    ].ewm(
        span=EMA_FAST,
        adjust=False
    ).mean()

    df["ema50"] = df[
        "close"
    ].ewm(
        span=EMA_SLOW,
        adjust=False
    ).mean()

    # --------------------------------------------------------
    # ADX
    # --------------------------------------------------------

    (
        df["adx"],
        df["plus_di"],
        df["minus_di"]
    ) = calculate_adx(
        df,
        ADX_PERIOD
    )

    previous = df.iloc[-2]

    current = df.iloc[-1]

    previous_ema20 = float(
        previous["ema20"]
    )

    previous_ema50 = float(
        previous["ema50"]
    )

    current_ema20 = float(
        current["ema20"]
    )

    current_ema50 = float(
        current["ema50"]
    )

    current_price = float(
        current["close"]
    )

    current_adx = float(
        current["adx"]
    )

    current_plus_di = float(
        current["plus_di"]
    )

    current_minus_di = float(
        current["minus_di"]
    )

    # --------------------------------------------------------
    # EMA DISTANCE
    # --------------------------------------------------------

    if current_ema50 == 0:

        return None

    ema_distance_percent = (
        abs(
            current_ema20
            - current_ema50
        )
        / current_ema50
    ) * 100

    # --------------------------------------------------------
    # ACTUAL CROSS
    # --------------------------------------------------------

    golden_cross = (
        previous_ema20
        <= previous_ema50
        and current_ema20
        > current_ema50
    )

    death_cross = (
        previous_ema20
        >= previous_ema50
        and current_ema20
        < current_ema50
    )

    # --------------------------------------------------------
    # IMPENDING CROSS
    # --------------------------------------------------------

    impending_golden = (
        current_ema20
        < current_ema50
        and ema_distance_percent
        <= IMPENDING_CROSS_PERCENT
    )

    impending_death = (
        current_ema20
        > current_ema50
        and ema_distance_percent
        <= IMPENDING_CROSS_PERCENT
    )

    if golden_cross:

        signal = "GOLDEN CROSS"

        direction = "BULLISH"

    elif death_cross:

        signal = "DEATH CROSS"

        direction = "BEARISH"

    elif impending_golden:

        signal = "IMPENDING GOLDEN CROSS"

        direction = "BULLISH"

    elif impending_death:

        signal = "IMPENDING DEATH CROSS"

        direction = "BEARISH"

    else:

        return None

    # --------------------------------------------------------
    # MARKET STRUCTURE
    # --------------------------------------------------------

    structure = get_market_structure(
        df
    )

    # --------------------------------------------------------
    # VOLUME
    # --------------------------------------------------------

    (
        volume_status,
        volume_ratio
    ) = get_volume_status(
        df
    )

    # --------------------------------------------------------
    # ADX TREND STRENGTH
    # --------------------------------------------------------

    if current_adx >= 25:

        adx_strength = "STRONG TREND"

    elif current_adx >= 20:

        adx_strength = "DEVELOPING TREND"

    else:

        adx_strength = "WEAK / RANGE"

    # --------------------------------------------------------
    # DI DIRECTION
    # --------------------------------------------------------

    if (
        current_plus_di
        > current_minus_di
    ):

        di_direction = "BULLISH"

    elif (
        current_minus_di
        > current_plus_di
    ):

        di_direction = "BEARISH"

    else:

        di_direction = "NEUTRAL"

    # --------------------------------------------------------
    # CANDLE STATUS
    # --------------------------------------------------------

    interval_ms = (
        30 * 60 * 1000
    )

    current_bucket = (
        int(time.time() * 1000)
        // interval_ms
    ) * interval_ms

    candle_timestamp = int(
        current["ts"]
    )

    if candle_timestamp >= current_bucket:

        candle_status = "FORMING"

    else:

        candle_status = "CLOSED"

    candle_time = datetime.fromtimestamp(
        candle_timestamp / 1000,
        tz=timezone.utc
    )

    # --------------------------------------------------------
    # CONFLUENCE
    # --------------------------------------------------------

    confirmations = 0

    if direction == "BULLISH":

        if structure == "BULLISH":

            confirmations += 1

        if (
            current_plus_di
            > current_minus_di
        ):

            confirmations += 1

    if direction == "BEARISH":

        if structure == "BEARISH":

            confirmations += 1

        if (
            current_minus_di
            > current_plus_di
        ):

            confirmations += 1

    if current_adx >= 20:

        confirmations += 1

    if volume_ratio >= 1.15:

        confirmations += 1

    if confirmations >= 3:

        confluence = "HIGHER CONFLUENCE"

    elif confirmations == 2:

        confluence = "MODERATE CONFLUENCE"

    else:

        confluence = "LOW CONFLUENCE"

    return {
        "symbol": symbol,
        "signal": signal,
        "direction": direction,
        "status": candle_status,
        "candle_time": candle_time.strftime(
            "%Y-%m-%d %H:%M UTC"
        ),
        "price": current_price,
        "ema20": current_ema20,
        "ema50": current_ema50,
        "ema_distance": ema_distance_percent,
        "adx": current_adx,
        "plus_di": current_plus_di,
        "minus_di": current_minus_di,
        "adx_strength": adx_strength,
        "di_direction": di_direction,
        "structure": structure,
        "volume_status": volume_status,
        "volume_ratio": volume_ratio,
        "confluence": confluence
    }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:

        print(
            "ERROR: TELEGRAM_BOT_TOKEN is missing."
        )

        return False

    if not TELEGRAM_CHAT_ID:

        print(
            "ERROR: TELEGRAM_CHAT_ID is missing."
        )

        return False

    url = TELEGRAM_SEND.format(
        TELEGRAM_BOT_TOKEN
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }

    try:

        response = session.post(
            url,
            data=payload,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):

            print(
                "Telegram error:",
                result
            )

            return False

        return True

    except requests.RequestException as error:

        print(
            "Telegram error:",
            error
        )

        return False


# ============================================================
# FORMAT TELEGRAM ALERT
# ============================================================

def format_message(signal):

    if signal["direction"] == "BULLISH":

        emoji = "🟢"

    else:

        emoji = "🔴"

    message = (
        f"{emoji} {signal['signal']}\n\n"

        f"Pair: {signal['symbol']}\n"
        f"Timeframe: 30 MINUTES\n"
        f"Candle: {signal['status']}\n"
        f"Time: {signal['candle_time']}\n\n"

        f"PRICE\n"
        f"{signal['price']:.8g}\n\n"

        f"EMA\n"
        f"EMA20: {signal['ema20']:.8g}\n"
        f"EMA50: {signal['ema50']:.8g}\n"
        f"Distance: "
        f"{signal['ema_distance']:.4f}%\n\n"

        f"ADX\n"
        f"ADX: {signal['adx']:.2f}\n"
        f"+DI: {signal['plus_di']:.2f}\n"
        f"-DI: {signal['minus_di']:.2f}\n"
        f"Trend: {signal['adx_strength']}\n"
        f"DI Direction: {signal['di_direction']}\n\n"

        f"MARKET STRUCTURE\n"
        f"{signal['structure']}\n\n"

        f"VOLUME\n"
        f"Status: {signal['volume_status']}\n"
        f"Ratio: "
        f"{signal['volume_ratio']:.2f}x average\n\n"

        f"CONFLUENCE\n"
        f"{signal['confluence']}\n\n"

        f"Bitget Spot"
    )

    return message


# ============================================================
# PROCESS ONE SYMBOL
#
# This wrapper allows ThreadPoolExecutor to process
# many symbols simultaneously.
# ============================================================

def process_symbol(symbol):

    try:

        result = check_setup(
            symbol
        )

        return (
            symbol,
            result,
            None
        )

    except Exception as error:

        return (
            symbol,
            None,
            str(error)
        )


# ============================================================
# SCAN ALL PAIRS CONCURRENTLY
# ============================================================

def scan_market():

    print("=" * 70)

    print(
        "BITGET EMA20 / EMA50 + ADX + "
        "STRUCTURE + VOLUME"
    )

    print(
        "FAST CONCURRENT 30-MINUTE "
        "FORMING-CANDLE SCANNER"
    )

    print("=" * 70)

    scan_start = time.monotonic()

    stamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print(
        "Scan started:",
        stamp
    )

    # --------------------------------------------------------
    # GET CURRENT ONLINE SYMBOLS
    # --------------------------------------------------------

    symbols = get_symbols()

    print(
        f"Found {len(symbols)} "
        f"online USDT Spot pairs."
    )

    print(
        f"Concurrent workers: "
        f"{MAX_WORKERS}"
    )

    print(
        f"Request limit: "
        f"{REQUESTS_PER_SECOND}/second"
    )

    print()

    alerts_sent = 0

    setups_found = 0

    failed = 0

    completed = 0

    # --------------------------------------------------------
    # CONCURRENT SCANNING
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                process_symbol,
                symbol
            ): symbol
            for symbol in symbols
        }

        for future in as_completed(
            futures
        ):

            symbol = futures[
                future
            ]

            completed += 1

            try:

                (
                    returned_symbol,
                    result,
                    error
                ) = future.result()

                if error:

                    failed += 1

                    print(
                        f"{symbol}: {error}"
                    )

                    continue

                if result:

                    setups_found += 1

                    print(
                        f"{result['signal']} | "
                        f"{result['status']} | "
                        f"{symbol} | "
                        f"ADX "
                        f"{result['adx']:.1f} | "
                        f"Structure "
                        f"{result['structure']} | "
                        f"Volume "
                        f"{result['volume_status']}"
                    )

                    # ------------------------------------------------
                    # ALERT DEDUPLICATION
                    # ------------------------------------------------

                    state_key = (
                        result["signal"]
                        + "|"
                        + result["status"]
                    )

                    with alert_lock:

                        previous_state = (
                            last_alert_state.get(
                                symbol
                            )
                        )

                        should_alert = (
                            previous_state
                            != state_key
                        )

                        if should_alert:

                            # Reserve the state before sending.
                            #
                            # This prevents duplicate alerts if
                            # multiple processing events occur.
                            last_alert_state[
                                symbol
                            ] = state_key

                    if should_alert:

                        message = format_message(
                            result
                        )

                        if send_telegram(
                            message
                        ):

                            alerts_sent += 1

                            print(
                                "Telegram alert sent:",
                                symbol
                            )

                        else:

                            # If Telegram failed, remove the
                            # reserved state so the next scan
                            # can retry the alert.
                            with alert_lock:

                                last_alert_state.pop(
                                    symbol,
                                    None
                                )

                else:

                    # No active setup.
                    #
                    # Clear the previous state so a future
                    # setup can generate a new alert.
                    with alert_lock:

                        last_alert_state.pop(
                            symbol,
                            None
                        )

            except Exception as error:

                failed += 1

                print(
                    f"{symbol}: worker error: "
                    f"{error}"
                )

            # ------------------------------------------------
            # PROGRESS
            # ------------------------------------------------

            if (
                completed % 50 == 0
                or completed == len(symbols)
            ):

                elapsed = (
                    time.monotonic()
                    - scan_start
                )

                print(
                    f"Progress: "
                    f"{completed}/"
                    f"{len(symbols)} "
                    f"({elapsed:.1f}s)"
                )

    # --------------------------------------------------------
    # SCAN SUMMARY
    # --------------------------------------------------------

    elapsed = (
        time.monotonic()
        - scan_start
    )

    print()

    print(
        "-" * 70
    )

    print(
        f"Pairs scanned: "
        f"{completed}/{len(symbols)}"
    )

    print(
        f"Setups found: "
        f"{setups_found}"
    )

    print(
        f"Failed requests: "
        f"{failed}"
    )

    print(
        f"Alerts sent: "
        f"{alerts_sent}"
    )

    print(
        f"FULL SCAN TIME: "
        f"{elapsed:.2f} seconds"
    )

    if elapsed <= 30:

        print(
            "STATUS: FAST ENOUGH FOR "
            "30-SECOND SCANNING"
        )

    elif elapsed <= 60:

        print(
            "STATUS: UNDER 1 MINUTE"
        )

    else:

        print(
            "STATUS: STILL OVER 1 MINUTE"
        )

    print(
        "-" * 70
    )

    print(
        "SCAN COMPLETE"
    )

    print()

    return elapsed


# ============================================================
# CONTINUOUS LOOP
# ============================================================

def main():

    print()

    print(
        "======================================================"
    )

    print(
        "BITGET REAL-TIME EMA SCANNER"
    )

    print(
        "======================================================"
    )

    print(
        "Scan target: EVERY 30 SECONDS"
    )

    print(
        "Timeframe: 30 MINUTES"
    )

    print(
        "EMA20/EMA50: ENABLED"
    )

    print(
        "ADX: ENABLED"
    )

    print(
        "MARKET STRUCTURE: ENABLED"
    )

    print(
        "VOLUME: ENABLED"
    )

    print(
        "RSI: DISABLED"
    )

    print(
        "FORMING CANDLE: ENABLED"
    )

    print(
        "CLOSED CANDLE: ENABLED"
    )

    print(
        f"CONCURRENT WORKERS: "
        f"{MAX_WORKERS}"
    )

    print()

    while True:

        cycle_start = time.monotonic()

        try:

            elapsed = scan_market()

        except Exception as error:

            print(
                "SCAN ERROR:",
                error
            )

            elapsed = (
                time.monotonic()
                - cycle_start
            )

        # ----------------------------------------------------
        # NEVER OVERLAP SCANS
        #
        # If scan takes 30 seconds or more, immediately start
        # the next scan.
        # ----------------------------------------------------

        wait_time = max(
            0,
            SCAN_INTERVAL - elapsed
        )

        if wait_time > 0:

            next_scan = datetime.fromtimestamp(
                time.time() + wait_time,
                tz=timezone.utc
            ).strftime(
                "%Y-%m-%d %H:%M:%S UTC"
            )

            print(
                f"Next scan: "
                f"{next_scan}"
            )

            print(
                f"Waiting "
                f"{wait_time:.1f} seconds..."
            )

            time.sleep(
                wait_time
            )

        else:

            print(
                "Scan exceeded target interval."
            )

            print(
                "Starting next scan immediately..."
            )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
