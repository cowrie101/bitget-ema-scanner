import json
import os
import time
import threading
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# BITGET REAL-TIME EMA 20 / EMA 50 SCANNER
#
# WEBSOCKET FORMING-CANDLE VERSION
#
# Bitget Spot
# 30-minute candles
#
# ADX       : OFF
# VOLUME    : OFF
# STRUCTURE : OFF
#
# Market / Trend Condition : ON
# Forming Candle            : ON
# ============================================================


# ============================================================
# BITGET ENDPOINTS
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

MAX_HISTORY = 120


# ------------------------------------------------------------
# IMPENDING CROSS
#
# Example:
#
# EMA20 is below EMA50.
# If distance becomes <= 0.15% and the gap is closing,
# we consider a Golden Cross to be developing.
# ------------------------------------------------------------

IMPENDING_CROSS_PERCENT = 0.15


# ------------------------------------------------------------
# TREND REQUIREMENT
#
# We deliberately do NOT use ADX.
#
# Trend is determined from:
#
# EMA20 vs EMA50
# EMA20 slope
# EMA50 slope
#
# This keeps the calculation lightweight.
# ------------------------------------------------------------

TREND_LOOKBACK = 3

MIN_TREND_SLOPE_PERCENT = 0.01


# ------------------------------------------------------------
# WEBSOCKET
# ------------------------------------------------------------

SYMBOLS_PER_WS = 40

RECONNECT_DELAY = 5

PING_SECONDS = 25


# ------------------------------------------------------------
# INITIAL HISTORY
# ------------------------------------------------------------

HISTORY_WORKERS = 12

REQUEST_TIMEOUT = 15

MAX_RETRIES = 3


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

candles_lock = threading.RLock()


# ------------------------------------------------------------
# Last alert state
#
# Structure:
#
# {
#     "BTCUSDT": {
#         "IMPENDING GOLDEN CROSS": timestamp,
#         "GOLDEN CROSS": timestamp
#     }
# }
# ------------------------------------------------------------

last_alert = {}

alert_lock = threading.Lock()


# ------------------------------------------------------------
# Diagnostic counters
# ------------------------------------------------------------

ws_updates = 0

ws_updates_lock = threading.Lock()


# ------------------------------------------------------------
# Stop event
# ------------------------------------------------------------

stop_event = threading.Event()


# ------------------------------------------------------------
# Telegram lock
# ------------------------------------------------------------

telegram_lock = threading.Lock()


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Bitget-EMA-WebSocket-Scanner/5.0"
})


# ============================================================
# TIME
# ============================================================

def now_utc():

    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


# ============================================================
# GET SYMBOLS
# ============================================================

def get_symbols():

    print(
        "Getting Bitget Spot USDT symbols..."
    )

    response = session.get(
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

    symbols = sorted(
        set(symbols)
    )

    return symbols


# ============================================================
# GET HISTORICAL CANDLES
#
# Used ONLY to initialize EMA history.
#
# After initialization, WebSocket supplies the forming candle.
# ============================================================

def get_history(symbol):

    params = {
        "symbol": symbol,
        "granularity": TIMEFRAME,
        "limit": str(HISTORY_LIMIT)
    }

    for attempt in range(
        MAX_RETRIES
    ):

        try:

            response = session.get(
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
                    1 + attempt
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

            if len(result) < (
                EMA_SLOW
                + TREND_LOOKBACK
                + 5
            ):

                return (
                    symbol,
                    None
                )

            return (
                symbol,
                result[-MAX_HISTORY:]
            )

        except requests.RequestException as error:

            if attempt == MAX_RETRIES - 1:

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
                1 + attempt
            )

    return (
        symbol,
        None
    )


# ============================================================
# LOAD INITIAL HISTORY
# ============================================================

def load_history(symbols):

    print(
        f"Loading initial EMA history "
        f"for {len(symbols)} pairs..."
    )

    good = 0

    failed = 0

    with ThreadPoolExecutor(
        max_workers=HISTORY_WORKERS
    ) as executor:

        jobs = [
            executor.submit(
                get_history,
                symbol
            )
            for symbol in symbols
        ]

        for number, job in enumerate(
            as_completed(jobs),
            1
        ):

            try:

                symbol, data = (
                    job.result()
                )

            except Exception as error:

                failed += 1

                print(
                    f"History worker error: "
                    f"{error}"
                )

                continue

            if data:

                with candles_lock:

                    candles[symbol] = deque(
                        data,
                        maxlen=MAX_HISTORY
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
                    f"ready {good} | "
                    f"failed {failed}"
                )

    print()

    print(
        f"Initial history ready: "
        f"{good}/{len(symbols)}"
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
            (price - ema)
            * multiplier
            + ema
        )

    return ema


# ============================================================
# TREND CONDITION
#
# No ADX.
# No volume.
# No market structure.
#
# We use EMA relationship and EMA slopes.
# ============================================================

def get_market_condition(
    closes
):

    required = (
        EMA_SLOW
        + TREND_LOOKBACK
        + 2
    )

    if len(closes) < required:

        return (
            "UNCLEAR",
            "NONE"
        )

    ema20_current = calculate_ema(
        closes,
        EMA_FAST
    )

    ema50_current = calculate_ema(
        closes,
        EMA_SLOW
    )

    if (
        ema20_current is None
        or ema50_current is None
    ):

        return (
            "UNCLEAR",
            "NONE"
        )

    # --------------------------------------------------------
    # EMA values a few candles back
    # --------------------------------------------------------

    previous_closes = closes[
        :-TREND_LOOKBACK
    ]

    ema20_previous = calculate_ema(
        previous_closes,
        EMA_FAST
    )

    ema50_previous = calculate_ema(
        previous_closes,
        EMA_SLOW
    )

    if (
        ema20_previous is None
        or ema50_previous is None
    ):

        return (
            "UNCLEAR",
            "NONE"
        )

    # --------------------------------------------------------
    # Slopes
    # --------------------------------------------------------

    if ema20_previous == 0:

        return (
            "UNCLEAR",
            "NONE"
        )

    if ema50_previous == 0:

        return (
            "UNCLEAR",
            "NONE"
        )

    ema20_slope = (
        (
            ema20_current
            - ema20_previous
        )
        / ema20_previous
    ) * 100

    ema50_slope = (
        (
            ema50_current
            - ema50_previous
        )
        / ema50_previous
    ) * 100

    # --------------------------------------------------------
    # BULLISH TREND
    # --------------------------------------------------------

    if (
        ema20_current > ema50_current
        and ema20_slope
        >= MIN_TREND_SLOPE_PERCENT
        and ema50_slope
        >= 0
    ):

        return (
            "TRENDING",
            "BULLISH"
        )

    # --------------------------------------------------------
    # BEARISH TREND
    # --------------------------------------------------------

    if (
        ema20_current < ema50_current
        and ema20_slope
        <= -MIN_TREND_SLOPE_PERCENT
        and ema50_slope
        <= 0
    ):

        return (
            "TRENDING",
            "BEARISH"
        )

    # --------------------------------------------------------
    # Everything else is sideways / unclear.
    # --------------------------------------------------------

    return (
        "SIDEWAYS / UNCLEAR",
        "NONE"
    )


# ============================================================
# ANALYZE FORMING CANDLE
# ============================================================

def analyze(
    symbol,
    candle
):

    with candles_lock:

        history = candles.get(
            symbol
        )

        if not history:

            return None

        items = list(
            history
        )

    # --------------------------------------------------------
    # Replace current forming candle
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

        items.append(
            candle
        )

    else:

        return None

    items = items[-MAX_HISTORY:]

    closes = [
        x["close"]
        for x in items
    ]

    if len(closes) < (
        EMA_SLOW
        + TREND_LOOKBACK
        + 2
    ):

        return None

    # --------------------------------------------------------
    # CURRENT EMA VALUES
    #
    # IMPORTANT:
    # The CURRENT FORMING CANDLE is included.
    # --------------------------------------------------------

    ema20 = calculate_ema(
        closes,
        EMA_FAST
    )

    ema50 = calculate_ema(
        closes,
        EMA_SLOW
    )

    if (
        ema20 is None
        or ema50 is None
    ):

        return None

    # --------------------------------------------------------
    # Previous candle EMA
    #
    # Used to detect actual crossover.
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

    if ema50 == 0:

        return None

    distance = (
        abs(
            ema20 - ema50
        )
        / abs(ema50)
    ) * 100

    # --------------------------------------------------------
    # ACTUAL CROSS
    # --------------------------------------------------------

    golden_cross = (
        previous_ema20
        <= previous_ema50
        and ema20
        > ema50
    )

    death_cross = (
        previous_ema20
        >= previous_ema50
        and ema20
        < ema50
    )

    # --------------------------------------------------------
    # GAP DIRECTION
    # --------------------------------------------------------

    previous_gap = (
        previous_ema20
        - previous_ema50
    )

    current_gap = (
        ema20
        - ema50
    )

    gap_is_closing = (
        abs(current_gap)
        < abs(previous_gap)
    )

    # --------------------------------------------------------
    # IMPENDING CROSS
    # --------------------------------------------------------

    impending_golden = (
        ema20 < ema50
        and distance
        <= IMPENDING_CROSS_PERCENT
        and gap_is_closing
    )

    impending_death = (
        ema20 > ema50
        and distance
        <= IMPENDING_CROSS_PERCENT
        and gap_is_closing
    )

    # --------------------------------------------------------
    # MARKET CONDITION
    # --------------------------------------------------------

    market_condition, trend = (
        get_market_condition(
            closes
        )
    )

    # --------------------------------------------------------
    # ONLY TRENDING MARKETS
    #
    # No sideways signals.
    # --------------------------------------------------------

    if market_condition != "TRENDING":

        return None

    # --------------------------------------------------------
    # SIGNAL
    # --------------------------------------------------------

    if golden_cross:

        signal = "GOLDEN CROSS"

        direction = "BULLISH"

    elif death_cross:

        signal = "DEATH CROSS"

        direction = "BEARISH"

    elif impending_golden:

        signal = (
            "IMPENDING GOLDEN CROSS"
        )

        direction = "BULLISH"

    elif impending_death:

        signal = (
            "IMPENDING DEATH CROSS"
        )

        direction = "BEARISH"

    else:

        return None

    # --------------------------------------------------------
    # Verify direction agrees with trend.
    # --------------------------------------------------------

    if direction != trend:

        return None

    # --------------------------------------------------------
    # Candle time
    # --------------------------------------------------------

    candle_time = datetime.fromtimestamp(
        candle["ts"] / 1000,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    return {
        "symbol": symbol,
        "signal": signal,
        "direction": direction,
        "market_condition": market_condition,
        "trend": trend,
        "price": candle["close"],
        "ema20": ema20,
        "ema50": ema50,
        "distance": distance,
        "candle_time": candle_time,
        "candle_status": "FORMING",
        "ts": candle["ts"]
    }


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(
    message
):

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
                        message
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
            f"Telegram error: "
            f"{error}"
        )

        return False


# ============================================================
# FORMAT TELEGRAM MESSAGE
# ============================================================

def format_message(
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

        f"Exchange: Bitget Spot\n"

        f"Timeframe: 30 MINUTES\n"

        f"Candle: FORMING\n"

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

        f"TREND\n"
        f"{result['trend']}\n\n"

        f"⚡ FORMING CANDLE SIGNAL\n"

        f"Bitget Spot"
    )


# ============================================================
# HANDLE LIVE CANDLE
# ============================================================

def handle_candle(
    symbol,
    row,
    ws_number
):

    global ws_updates

    if len(row) < 5:

        return

    try:

        candle = {
            "ts": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4])
        }

    except (
        TypeError,
        ValueError
    ):

        return

    # --------------------------------------------------------
    # Store the live candle.
    # --------------------------------------------------------

    with candles_lock:

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

    # --------------------------------------------------------
    # Diagnostic counter
    # --------------------------------------------------------

    with ws_updates_lock:

        ws_updates += 1

        update_number = ws_updates

    # --------------------------------------------------------
    # PROVE FORMING CANDLE IS BEING RECEIVED
    #
    # We print the first update for every symbol and then
    # occasional updates so the GitHub log doesn't become huge.
    # --------------------------------------------------------

    if (
        update_number <= 100
        or update_number % 250 == 0
    ):

        print(
            f"WS LIVE | "
            f"{symbol} | "
            f"FORMING 30m CANDLE | "
            f"Price {candle['close']:.8g}"
        )

    # --------------------------------------------------------
    # Analyze
    # --------------------------------------------------------

    result = analyze(
        symbol,
        candle
    )

    if not result:

        return

    # --------------------------------------------------------
    # Alert deduplication
    #
    # One alert of a particular type per candle.
    # --------------------------------------------------------

    alert_key = (
        result["signal"]
    )

    with alert_lock:

        symbol_alerts = last_alert.setdefault(
            symbol,
            {}
        )

        previous_timestamp = (
            symbol_alerts.get(
                alert_key
            )
        )

        if previous_timestamp == result["ts"]:

            return

        symbol_alerts[
            alert_key
        ] = result["ts"]

    # --------------------------------------------------------
    # Telegram
    # --------------------------------------------------------

    message = format_message(
        result
    )

    if send_telegram(
        message
    ):

        print(
            f"🚨 TELEGRAM SENT | "
            f"{result['signal']} | "
            f"{symbol} | "
            f"FORMING | "
            f"{result['trend']}"
        )

    else:

        # Allow retry if Telegram failed.
        with alert_lock:

            last_alert.get(
                symbol,
                {}
            ).pop(
                alert_key,
                None
            )


# ============================================================
# WEBSOCKET
# ============================================================

def run_websocket(
    batch,
    ws_number
):

    while not stop_event.is_set():

        heartbeat_stop = (
            threading.Event()
        )

        def on_open(ws):

            subscriptions = []

            for symbol in batch:

                subscriptions.append({
                    "instType": "SPOT",
                    "channel": WS_CHANNEL,
                    "instId": symbol
                })

            ws.send(
                json.dumps({
                    "op": "subscribe",
                    "args": subscriptions
                })
            )

            print(
                f"WS {ws_number}: LIVE | "
                f"Subscribed to "
                f"{len(batch)} pairs"
            )

            # ------------------------------------------------
            # Heartbeat
            # ------------------------------------------------

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

            # ------------------------------------------------
            # Subscription confirmation
            # ------------------------------------------------

            if data.get("event") == "subscribe":

                arg = data.get(
                    "arg",
                    {}
                )

                print(
                    f"WS {ws_number}: "
                    f"SUBSCRIBED | "
                    f"{arg.get('instId', 'unknown')}"
                )

                return

            # ------------------------------------------------
            # Error
            # ------------------------------------------------

            if data.get("event") == "error":

                print(
                    f"WS {ws_number}: "
                    f"ERROR | "
                    f"{data}"
                )

                return

            # ------------------------------------------------
            # Candle update
            # ------------------------------------------------

            if data.get(
                "action"
            ) not in (
                "snapshot",
                "update"
            ):

                return

            arg = data.get(
                "arg",
                {}
            )

            symbol = arg.get(
                "instId"
            )

            channel = arg.get(
                "channel"
            )

            if (
                not symbol
                or channel != WS_CHANNEL
            ):

                return

            rows = data.get(
                "data",
                []
            )

            for row in rows:

                handle_candle(
                    symbol,
                    row,
                    ws_number
                )

        def on_error(
            ws,
            error
        ):

            print(
                f"WS {ws_number}: "
                f"ERROR | "
                f"{error}"
            )

        def on_close(
            ws,
            code,
            message
        ):

            heartbeat_stop.set()

            print(
                f"WS {ws_number}: "
                f"CLOSED | "
                f"{code} | "
                f"{message}"
            )

        try:

            app = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            app.run_forever(
                ping_interval=None,
                ping_timeout=None,
                skip_utf8_validation=True
            )

        except Exception as error:

            print(
                f"WS {ws_number}: "
                f"EXCEPTION | "
                f"{error}"
            )

        heartbeat_stop.set()

        if not stop_event.is_set():

            print(
                f"WS {ws_number}: "
                f"Reconnecting in "
                f"{RECONNECT_DELAY}s..."
            )

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
            i:i + SYMBOLS_PER_WS
        ]
        for i in range(
            0,
            len(symbols),
            SYMBOLS_PER_WS
        )
    ]

    print(
        f"Starting "
        f"{len(batches)} "
        f"WebSocket connections..."
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

        # Small stagger between connections.
        time.sleep(
            0.25
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
        "BITGET REAL-TIME EMA20 / EMA50 SCANNER"
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
        "EMA20: ENABLED"
    )

    print(
        "EMA50: ENABLED"
    )

    print(
        "FORMING CANDLE: ENABLED"
    )

    print(
        "WEBSOCKET: ENABLED"
    )

    print(
        "MARKET CONDITION: ENABLED"
    )

    print(
        "TREND CONDITION: ENABLED"
    )

    print(
        "ADX: OFF"
    )

    print(
        "VOLUME: OFF"
    )

    print(
        "MARKET STRUCTURE: OFF"
    )

    print(
        "SIDEWAYS SIGNALS: OFF"
    )

    print(
        f"Impending threshold: "
        f"{IMPENDING_CROSS_PERCENT}%"
    )

    print(
        "=" * 70
    )

    print()

    try:

        symbols = get_symbols()

        print(
            f"Found "
            f"{len(symbols)} "
            f"online USDT Spot pairs."
        )

        print()

        # ----------------------------------------------------
        # Initial history
        # ----------------------------------------------------

        load_history(
            symbols
        )

        # ----------------------------------------------------
        # WebSocket
        # ----------------------------------------------------

        connections = (
            start_websockets(
                symbols
            )
        )

        print()

        print(
            "=" * 70
        )

        print(
            "WEBSOCKET SCANNER IS NOW LIVE"
        )

        print(
            f"Pairs: {len(symbols)}"
        )

        print(
            f"WebSocket connections: "
            f"{connections}"
        )

        print(
            "Waiting for FORMING 30-minute "
            "candle updates..."
        )

        print(
            "=" * 70
        )

        print()

        # ----------------------------------------------------
        # Keep process alive.
        # ----------------------------------------------------

        while True:

            time.sleep(
                30
            )

            with ws_updates_lock:

                updates = ws_updates

            print(
                f"{now_utc()} | "
                f"WebSocket updates received: "
                f"{updates}"
            )

    except KeyboardInterrupt:

        stop_event.set()

        print(
            "Scanner stopped."
        )

    except Exception as error:

        print(
            f"MAIN ERROR: "
            f"{error}"
        )

        stop_event.set()

        raise


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
