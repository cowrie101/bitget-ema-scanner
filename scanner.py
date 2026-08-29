import json
import math
import os
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# BITGET EMA 20 / EMA 50 REAL-TIME SCANNER
#
# 30-MINUTE FORMING CANDLE
#
# ADX       = OFF
# VOLUME    = OFF
# STRUCTURE = OFF
#
# MARKET CONDITION = ON
# SIDEWAYS SIGNALS  = OFF
# ============================================================


# ============================================================
# BITGET API
# ============================================================

SYMBOLS_URL = (
    "https://api.bitget.com/api/v2/spot/public/symbols"
)

HISTORY_URL = (
    "https://api.bitget.com/api/v2/spot/market/history-candles"
)

WS_URL = (
    "wss://ws.bitget.com/v2/ws/public"
)

TELEGRAM_URL = (
    "https://api.telegram.org/bot{}/sendMessage"
)


# ============================================================
# SETTINGS
# ============================================================

TIMEFRAME = "30min"

WS_CHANNEL = "candle30m"

EMA_FAST = 20

EMA_SLOW = 50

HISTORY_LIMIT = 100

MAX_CANDLES = 100


# ============================================================
# IMPENDING CROSS SETTINGS
#
# Example:
#
# 0.10 means EMA20 must be within 0.10%
# of EMA50 to be considered close to crossing.
# ============================================================

IMPENDING_CROSS_PERCENT = 0.10


# ============================================================
# MARKET CONDITION
#
# We determine trend using:
#
# 1. EMA50 slope
# 2. EMA20 slope
# 3. Price position relative to EMA50
#
# This avoids using ADX, volume or structure.
# ============================================================

TREND_LOOKBACK = 3

MIN_TREND_SLOPE_PERCENT = 0.02


# ============================================================
# WEBSOCKET
# ============================================================

CHANNELS_PER_WS = 40

RECONNECT_DELAY = 5

PING_SECONDS = 25


# ============================================================
# REST HISTORY LOADING
# ============================================================

HISTORY_WORKERS = 12

REST_REQUESTS_PER_SECOND = 18

REQUEST_TIMEOUT = 15


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID"
)


# ============================================================
# GLOBAL STATE
# ============================================================

candles = {}

lock = threading.RLock()

stop_event = threading.Event()

telegram_lock = threading.Lock()


# ------------------------------------------------------------
# Prevent repeated alerts for the same candle/setup.
# ------------------------------------------------------------

last_alert = {}


# ------------------------------------------------------------
# Current signal state.
#
# This is deliberately separate from last_alert because
# the EMA relationship can move:
#
# IMPENDING -> CROSS
#
# or:
#
# IMPENDING -> disappears
# ------------------------------------------------------------

signal_state = {}


# ============================================================
# RATE LIMITER
# ============================================================

request_times = deque()

rate_lock = threading.Lock()


def wait_for_request_slot():

    while True:

        now = time.monotonic()

        with rate_lock:

            while (
                request_times
                and now - request_times[0] >= 1.0
            ):

                request_times.popleft()

            if len(request_times) < REST_REQUESTS_PER_SECOND:

                request_times.append(now)

                return

            wait_time = (
                1.0
                - (
                    now
                    - request_times[0]
                )
                + 0.01
            )

        time.sleep(
            max(0.01, wait_time)
        )


# ============================================================
# TIME
# ============================================================

def now_utc():

    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def candle_time_text(timestamp):

    return datetime.fromtimestamp(
        timestamp / 1000,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


# ============================================================
# BITGET SYMBOLS
# ============================================================

def get_symbols():

    response = requests.get(
        SYMBOLS_URL,
        timeout=REQUEST_TIMEOUT
    )

    response.raise_for_status()

    data = response.json()

    if data.get("code") != "00000":

        raise RuntimeError(
            f"Bitget symbols error: {data}"
        )

    symbols = []

    for item in data.get(
        "data",
        []
    ):

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

            symbols.append(
                symbol
            )

    return sorted(
        set(symbols)
    )


# ============================================================
# HISTORICAL CANDLES
# ============================================================

def get_history(symbol):

    params = {
        "symbol": symbol,
        "granularity": TIMEFRAME,
        "limit": str(HISTORY_LIMIT)
    }

    for attempt in range(3):

        try:

            wait_for_request_slot()

            response = requests.get(
                HISTORY_URL,
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

                time.sleep(
                    1.0 + attempt
                )

                continue

            if response.status_code == 400:

                return (
                    symbol,
                    None
                )

            response.raise_for_status()

            data = response.json()

            if data.get("code") != "00000":

                return (
                    symbol,
                    None
                )

            result = []

            for row in data.get(
                "data",
                []
            ):

                if len(row) < 5:

                    continue

                try:

                    result.append({
                        "ts": int(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                    })

                except (
                    TypeError,
                    ValueError
                ):

                    continue

            result.sort(
                key=lambda x: x["ts"]
            )

            minimum = (
                EMA_SLOW
                + TREND_LOOKBACK
                + 5
            )

            if len(result) < minimum:

                return (
                    symbol,
                    None
                )

            return (
                symbol,
                result[-MAX_CANDLES:]
            )

        except requests.RequestException as error:

            if attempt == 2:

                print(
                    f"{symbol}: "
                    f"history failed: "
                    f"{error}"
                )

                return (
                    symbol,
                    None
                )

            time.sleep(
                1.0 + attempt
            )

    return (
        symbol,
        None
    )


# ============================================================
# LOAD HISTORY
# ============================================================

def load_history(symbols):

    print(
        f"Loading historical candles "
        f"for {len(symbols)} pairs..."
    )

    good = 0

    failed = 0

    with ThreadPoolExecutor(
        max_workers=HISTORY_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                get_history,
                symbol
            ): symbol
            for symbol in symbols
        }

        for number, future in enumerate(
            as_completed(futures),
            1
        ):

            symbol = futures[
                future
            ]

            try:

                returned_symbol, data = (
                    future.result()
                )

            except Exception as error:

                failed += 1

                print(
                    f"{symbol}: "
                    f"history worker error: "
                    f"{error}"
                )

                continue

            if data:

                with lock:

                    candles[
                        returned_symbol
                    ] = deque(
                        data,
                        maxlen=MAX_CANDLES
                    )

                good += 1

            else:

                failed += 1

            if (
                number % 100 == 0
                or number == len(symbols)
            ):

                print(
                    f"History: "
                    f"{number}/"
                    f"{len(symbols)} | "
                    f"usable: {good} | "
                    f"failed: {failed}"
                )

    print()

    print(
        f"History ready: "
        f"{good}/{len(symbols)} pairs"
    )

    print()


# ============================================================
# EMA
# ============================================================

def calculate_ema(
    values,
    period
):

    if len(values) < period:

        return None

    multiplier = (
        2.0
        / (period + 1)
    )

    ema = (
        sum(
            values[:period]
        )
        / period
    )

    for price in values[period:]:

        ema = (
            (
                price - ema
            )
            * multiplier
        ) + ema

    return ema


# ============================================================
# MARKET CONDITION
#
# No ADX.
# No volume.
# No market structure.
#
# We use EMA slopes + price position.
# ============================================================

def get_market_condition(
    closes,
    current_ema20,
    current_ema50
):

    required = (
        EMA_SLOW
        + TREND_LOOKBACK
        + 2
    )

    if len(closes) < required:

        return "SIDEWAYS"

    # --------------------------------------------------------
    # EMA50 values at previous points.
    # --------------------------------------------------------

    ema50_values = []

    ema20_values = []

    for end in range(
        len(closes)
        - TREND_LOOKBACK
        - 1,
        len(closes) + 1
    ):

        section = closes[:end]

        value50 = calculate_ema(
            section,
            EMA_SLOW
        )

        value20 = calculate_ema(
            section,
            EMA_FAST
        )

        if (
            value50 is not None
            and value20 is not None
        ):

            ema50_values.append(
                value50
            )

            ema20_values.append(
                value20
            )

    if len(ema50_values) < 2:

        return "SIDEWAYS"

    old_ema50 = ema50_values[0]

    old_ema20 = ema20_values[0]

    if old_ema50 == 0 or old_ema20 == 0:

        return "SIDEWAYS"

    ema50_slope = (
        (
            current_ema50
            - old_ema50
        )
        / old_ema50
    ) * 100

    ema20_slope = (
        (
            current_ema20
            - old_ema20
        )
        / old_ema20
    ) * 100

    current_price = closes[-1]

    # --------------------------------------------------------
    # BULLISH TREND
    # --------------------------------------------------------

    if (
        ema50_slope
        >= MIN_TREND_SLOPE_PERCENT
        and ema20_slope
        > 0
        and current_price
        > current_ema50
    ):

        return "TRENDING UP"

    # --------------------------------------------------------
    # BEARISH TREND
    # --------------------------------------------------------

    if (
        ema50_slope
        <= -MIN_TREND_SLOPE_PERCENT
        and ema20_slope
        < 0
        and current_price
        < current_ema50
    ):

        return "TRENDING DOWN"

    return "SIDEWAYS"


# ============================================================
# ANALYZE CURRENT FORMING CANDLE
# ============================================================

def analyze_symbol(
    symbol,
    candle
):

    with lock:

        history = candles.get(
            symbol
        )

        if not history:

            return None

        items = list(history)

    # --------------------------------------------------------
    # Replace current candle or add new forming candle.
    # --------------------------------------------------------

    if (
        items
        and items[-1]["ts"]
        == candle["ts"]
    ):

        items[-1] = candle

    elif (
        not items
        or candle["ts"]
        > items[-1]["ts"]
    ):

        items.append(candle)

    else:

        return None

    items = items[-MAX_CANDLES:]

    closes = [
        item["close"]
        for item in items
    ]

    if len(closes) < (
        EMA_SLOW + 10
    ):

        return None

    # --------------------------------------------------------
    # EMA20 / EMA50
    # --------------------------------------------------------

    current_ema20 = calculate_ema(
        closes,
        EMA_FAST
    )

    current_ema50 = calculate_ema(
        closes,
        EMA_SLOW
    )

    if (
        current_ema20 is None
        or current_ema50 is None
    ):

        return None

    # --------------------------------------------------------
    # MARKET CONDITION
    # --------------------------------------------------------

    market_condition = get_market_condition(
        closes,
        current_ema20,
        current_ema50
    )

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # SIDEWAYS = NO SIGNAL
    # --------------------------------------------------------

    if market_condition == "SIDEWAYS":

        with lock:

            signal_state.pop(
                symbol,
                None
            )

        return None

    # --------------------------------------------------------
    # PREVIOUS EMA VALUES
    #
    # These are calculated using the previous candle.
    # --------------------------------------------------------

    previous_closes = closes[:-1]

    previous_ema20 = calculate_ema(
        previous_closes,
        EMA_FAST
    )

    previous_ema50 = calculate_ema(
        previous_closes,
        EMA_SLOW
    )

    if (
        previous_ema20 is None
        or previous_ema50 is None
    ):

        return None

    # --------------------------------------------------------
    # EMA DISTANCE
    # --------------------------------------------------------

    if current_ema50 == 0:

        return None

    distance = (
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
    #
    # GOLDEN:
    # EMA20 is below EMA50 and getting very close.
    #
    # DEATH:
    # EMA20 is above EMA50 and getting very close.
    # --------------------------------------------------------

    impending_golden = (
        current_ema20
        < current_ema50
        and distance
        <= IMPENDING_CROSS_PERCENT
        and market_condition
        == "TRENDING UP"
    )

    impending_death = (
        current_ema20
        > current_ema50
        and distance
        <= IMPENDING_CROSS_PERCENT
        and market_condition
        == "TRENDING DOWN"
    )

    # --------------------------------------------------------
    # ACTUAL CROSS DIRECTION MUST AGREE WITH TREND.
    # --------------------------------------------------------

    if (
        golden_cross
        and market_condition
        == "TRENDING UP"
    ):

        signal = "GOLDEN CROSS"

        direction = "BULLISH"

    elif (
        death_cross
        and market_condition
        == "TRENDING DOWN"
    ):

        signal = "DEATH CROSS"

        direction = "BEARISH"

    elif impending_golden:

        signal = "IMPENDING GOLDEN CROSS"

        direction = "BULLISH"

    elif impending_death:

        signal = "IMPENDING DEATH CROSS"

        direction = "BEARISH"

    else:

        with lock:

            signal_state.pop(
                symbol,
                None
            )

        return None

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

    if candle["ts"] >= current_bucket:

        candle_status = "FORMING"

    else:

        candle_status = "CLOSED"

    # --------------------------------------------------------
    # RETURN SIGNAL
    # --------------------------------------------------------

    return {
        "symbol": symbol,
        "signal": signal,
        "direction": direction,
        "market_condition": market_condition,
        "candle_status": candle_status,
        "candle_time": candle_time_text(
            candle["ts"]
        ),
        "price": candle["close"],
        "ema20": current_ema20,
        "ema50": current_ema50,
        "distance": distance,
        "timestamp": candle["ts"],
    }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):

        print(
            "Telegram secrets missing."
        )

        return False

    try:

        with telegram_lock:

            response = requests.post(
                TELEGRAM_URL.format(
                    TELEGRAM_BOT_TOKEN
                ),
                data={
                    "chat_id":
                        TELEGRAM_CHAT_ID,
                    "text":
                        message,
                },
                timeout=10
            )

        response.raise_for_status()

        result = response.json()

        return bool(
            result.get("ok")
        )

    except requests.RequestException as error:

        print(
            f"Telegram error: {error}"
        )

        return False


# ============================================================
# FORMAT TELEGRAM ALERT
# ============================================================

def format_alert(
    result
):

    if result["direction"] == "BULLISH":

        emoji = "🟢"

    else:

        emoji = "🔴"

    return (

        f"{emoji} "
        f"{result['signal']}\n\n"

        f"Pair: "
        f"{result['symbol']}\n"

        f"Timeframe: "
        f"30 MINUTES\n"

        f"Candle: "
        f"{result['candle_status']}\n"

        f"Time: "
        f"{result['candle_time']}\n\n"

        f"PRICE\n"
        f"{result['price']:.8g}\n\n"

        f"EMA\n"
        f"EMA20: "
        f"{result['ema20']:.8g}\n"

        f"EMA50: "
        f"{result['ema50']:.8g}\n"

        f"Distance: "
        f"{result['distance']:.4f}%\n\n"

        f"MARKET CONDITION\n"
        f"{result['market_condition']}\n\n"

        f"STATUS\n"
        f"FORMING CANDLE\n\n"

        f"Bitget Spot"
    )


# ============================================================
# HANDLE LIVE CANDLE
# ============================================================

def handle_candle(
    symbol,
    row
):

    if len(row) < 5:

        return

    try:

        candle = {
            "ts": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        }

    except (
        TypeError,
        ValueError
    ):

        return

    result = analyze_symbol(
        symbol,
        candle
    )

    # --------------------------------------------------------
    # Always save the newest live candle.
    # --------------------------------------------------------

    with lock:

        history = candles.get(
            symbol
        )

        if not history:

            return

        if (
            history[-1]["ts"]
            == candle["ts"]
        ):

            history[-1] = candle

        elif (
            candle["ts"]
            > history[-1]["ts"]
        ):

            history.append(
                candle
            )

    if not result:

        return

    # --------------------------------------------------------
    # FORMING CANDLE ONLY
    #
    # We do not want delayed closed-candle alerts.
    # --------------------------------------------------------

    if result["candle_status"] != "FORMING":

        return

    # --------------------------------------------------------
    # ALERT KEY
    #
    # Allows:
    #
    # IMPENDING GOLDEN
    # then
    # GOLDEN CROSS
    #
    # during the SAME forming candle.
    # --------------------------------------------------------

    alert_key = (
        result["signal"]
        + "|"
        + str(result["timestamp"])
    )

    with lock:

        previous_alert = last_alert.get(
            symbol
        )

        if previous_alert == alert_key:

            return

        # Reserve before Telegram.
        last_alert[
            symbol
        ] = alert_key

    # --------------------------------------------------------
    # SEND
    # --------------------------------------------------------

    message = format_alert(
        result
    )

    if send_telegram(
        message
    ):

        print(
            f"{now_utc()} | "
            f"{result['signal']} | "
            f"{symbol} | "
            f"{result['market_condition']} | "
            f"EMA20 "
            f"{result['ema20']:.8g} | "
            f"EMA50 "
            f"{result['ema50']:.8g}"
        )

    else:

        # Allow retry if Telegram fails.
        with lock:

            if last_alert.get(
                symbol
            ) == alert_key:

                last_alert.pop(
                    symbol,
                    None
                )


# ============================================================
# WEBSOCKET
# ============================================================

def run_websocket(
    batch,
    number
):

    while not stop_event.is_set():

        heartbeat_stop = (
            threading.Event()
        )

        def on_open(ws):

            args = [
                {
                    "instType": "SPOT",
                    "channel": WS_CHANNEL,
                    "instId": symbol,
                }
                for symbol in batch
            ]

            ws.send(
                json.dumps({
                    "op": "subscribe",
                    "args": args
                })
            )

            print(
                f"WS {number}: "
                f"LIVE | "
                f"{len(batch)} pairs"
            )

            def heartbeat():

                while not heartbeat_stop.wait(
                    PING_SECONDS
                ):

                    try:

                        ws.send(
                            "ping"
                        )

                    except Exception:

                        break

            threading.Thread(
                target=heartbeat,
                daemon=True
            ).start()

        def on_message(
            ws,
            message
        ):

            if message == "pong":

                return

            try:

                data = json.loads(
                    message
                )

            except (
                TypeError,
                ValueError
            ):

                return

            if (
                data.get("event")
                == "error"
            ):

                print(
                    f"WS {number} "
                    f"error: {data}"
                )

                return

            if data.get(
                "action"
            ) not in (
                "snapshot",
                "update"
            ):

                return

            symbol = (
                data
                .get("arg", {})
                .get("instId")
            )

            if not symbol:

                return

            rows = data.get(
                "data",
                []
            )

            for row in rows:

                handle_candle(
                    symbol,
                    row
                )

        def on_error(
            ws,
            error
        ):

            print(
                f"WS {number} error: "
                f"{error}"
            )

        def on_close(
            ws,
            code,
            message
        ):

            heartbeat_stop.set()

            print(
                f"WS {number} closed: "
                f"{code} "
                f"{message}"
            )

        try:

            app = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            app.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

        except Exception as error:

            print(
                f"WS {number} exception: "
                f"{error}"
            )

        heartbeat_stop.set()

        if not stop_event.is_set():

            time.sleep(
                RECONNECT_DELAY
            )


# ============================================================
# START WEBSOCKETS
# ============================================================

def start_websockets(
    symbols
):

    batches = [
        symbols[
            i:i + CHANNELS_PER_WS
        ]
        for i in range(
            0,
            len(symbols),
            CHANNELS_PER_WS
        )
    ]

    print(
        f"Starting "
        f"{len(batches)} "
        f"live WebSocket connections..."
    )

    for number, batch in enumerate(
        batches,
        1
    ):

        threading.Thread(
            target=run_websocket,
            args=(
                batch,
                number
            ),
            daemon=True
        ).start()

        time.sleep(
            0.15
        )

    return len(batches)


# ============================================================
# MAIN
# ============================================================

def main():

    print()

    print(
        "=" * 70
    )

    print(
        "BITGET EMA20 / EMA50 "
        "REAL-TIME SCANNER"
    )

    print(
        "=" * 70
    )

    print(
        "Exchange: BITGET SPOT"
    )

    print(
        "Timeframe: 30 MINUTES"
    )

    print(
        "EMA FAST: 20"
    )

    print(
        "EMA SLOW: 50"
    )

    print()

    print(
        "FORMING CANDLE: ON"
    )

    print(
        "IMPENDING CROSS: ON"
    )

    print(
        "MARKET CONDITION: ON"
    )

    print(
        "SIDEWAYS SIGNALS: OFF"
    )

    print()

    print(
        "ADX: OFF"
    )

    print(
        "VOLUME: OFF"
    )

    print(
        "MARKET STRUCTURE: OFF"
    )

    print()

    print(
        "Watching TRENDING MARKETS ONLY"
    )

    print(
        "=" * 70
    )

    while not stop_event.is_set():

        try:

            symbols = get_symbols()

            print()

            print(
                f"Found "
                f"{len(symbols)} "
                f"online USDT Spot pairs."
            )

            load_history(
                symbols
            )

            connections = (
                start_websockets(
                    symbols
                )
            )

            print()

            print(
                "=================================================="
            )

            print(
                f"SCANNER LIVE | "
                f"{len(symbols)} pairs | "
                f"{connections} WebSockets"
            )

            print(
                "REAL-TIME FORMING 30-MINUTE CANDLES"
            )

            print(
                "TRENDING MARKETS ONLY"
            )

            print(
                "IMPENDING + ACTUAL CROSSOVERS"
            )

            print(
                "=================================================="
            )

            # ------------------------------------------------
            # Keep process alive.
            # ------------------------------------------------

            while not stop_event.is_set():

                time.sleep(
                    30
                )

        except KeyboardInterrupt:

            stop_event.set()

            print(
                "Scanner stopped."
            )

            return

        except Exception as error:

            print()

            print(
                f"MAIN ERROR: {error}"
            )

            print(
                "Retrying in 10 seconds..."
            )

            time.sleep(
                10
            )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
