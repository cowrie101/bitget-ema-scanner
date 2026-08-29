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

============================================================

BITGET EMA20 / EMA50 REAL-TIME WEBSOCKET SCANNER



SPOT USDT PAIRS

30-MINUTE CANDLES



FEATURES

------------------------------------------------------------

EMA 20

EMA 50

Actual Golden Cross

Actual Death Cross

Impending Golden Cross

Impending Death Cross

Forming candle

Market condition

Trend condition

Market structure

Telegram alerts



REMOVED

------------------------------------------------------------

ADX

+DI / -DI

Volume

Volume ratio

Bollinger Bands

RSI

Squeeze



SIDEWAYS MARKETS ARE EXCLUDED

============================================================

============================================================

BITGET REST API

============================================================

BITGET_SYMBOLS = (
"https://api.bitget.com/api/v2/spot/public/symbols"
)

BITGET_CANDLES = (
"https://api.bitget.com/api/v2/spot/market/history-candles"
)

============================================================

BITGET WEBSOCKET

============================================================

BITGET_WS_URL = (
"wss://ws.bitget.com/v2/ws/public"
)

============================================================

TELEGRAM

============================================================

TELEGRAM_SEND = (
"https://api.telegram.org/bot{}/sendMessage"
)

============================================================

SETTINGS

============================================================

EMA_FAST = 20
EMA_SLOW = 50

GRANULARITY = "30min"

CANDLE_LIMIT = 200

============================================================

IMPENDING CROSS

============================================================

IMPENDING_CROSS_PERCENT = 0.15

============================================================

TREND SETTINGS

============================================================

TREND_LOOKBACK = 5

MIN_EMA50_SLOPE_PERCENT = 0.03

MIN_EMA20_SLOPE_PERCENT = 0.03

============================================================

MARKET STRUCTURE

============================================================

STRUCTURE_LOOKBACK = 5

============================================================

WEBSOCKET SETTINGS

============================================================

WS_PAIRS_PER_CONNECTION = 40

WS_PING_INTERVAL = 20

WS_RECONNECT_DELAY = 5

============================================================

REST SETTINGS

============================================================

REQUEST_TIMEOUT = 10

MAX_RETRIES = 3

MAX_WORKERS = 12

REQUESTS_PER_SECOND = 19

============================================================

TELEGRAM SECRETS

============================================================

TELEGRAM_BOT_TOKEN = os.environ.get(
"TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.environ.get(
"TELEGRAM_CHAT_ID"
)

============================================================

HTTP SESSION

============================================================

session = requests.Session()

session.headers.update({
"User-Agent":
"Bitget-EMA20-EMA50-WebSocket-Scanner/6.0"
})

============================================================

THREAD-LOCAL HTTP SESSIONS

============================================================

thread_local = threading.local()

def get_thread_session():

if not hasattr(  
    thread_local,  
    "session"  
):  

    thread_local.session = requests.Session()  

    thread_local.session.headers.update({  
        "User-Agent":  
            "Bitget-EMA20-EMA50-WebSocket-Scanner/6.0"  
    })  

return thread_local.session

============================================================

RATE LIMITER

============================================================

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

============================================================

SHARED CANDLE STORAGE

============================================================

candles = {}

candles_lock = threading.RLock()

============================================================

WEBSOCKET STATUS

============================================================

ws_updates = 0

ws_data_messages = 0

ws_candle_messages = 0

ws_connections_live = 0

ws_subscriptions = 0

ws_lock = threading.Lock()

============================================================

LAST ALERT STATE

============================================================

last_alert_state = {}

alert_lock = threading.Lock()

============================================================

GET ALL ONLINE USDT SPOT PAIRS

============================================================

def get_symbols():

print(  
    "Getting Bitget Spot USDT symbols..."  
)  

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

print(  
    f"Found {len(symbols)} online "  
    f"USDT Spot pairs."  
)  

return symbols

============================================================

GET HISTORICAL CANDLES

============================================================

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

============================================================

CREATE DATAFRAME

============================================================

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
    keep="last"  
)  

df = df.sort_values(  
    "ts"  
).reset_index(  
    drop=True  
)  

return df

============================================================

LOAD ONE SYMBOL'S HISTORY

============================================================

def initialize_symbol(symbol):

rows = get_historical_candles(  
    symbol  
)  

if not rows:  

    return symbol, False  

df = make_dataframe(  
    rows  
)  

if df.empty:  

    return symbol, False  

if len(df) < EMA_SLOW + 10:  

    return symbol, False  

with candles_lock:  

    candles[symbol] = deque(  
        df.to_dict(  
            "records"  
        ),  
        maxlen=CANDLE_LIMIT  
    )  

return symbol, True

============================================================

INITIALIZE ALL SYMBOLS

============================================================

def initialize_history(symbols):

print()  
print("=" * 70)  
print(  
    "INITIALIZING HISTORICAL EMA DATA"  
)  
print("=" * 70)  

successful = 0  

failed = 0  

completed = 0  

start = time.monotonic()  

with ThreadPoolExecutor(  
    max_workers=MAX_WORKERS  
) as executor:  

    futures = {  
        executor.submit(  
            initialize_symbol,  
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

            returned_symbol, success = (  
                future.result()  
            )  

            if success:  

                successful += 1  

            else:  

                failed += 1  

        except Exception as error:  

            failed += 1  

            print(  
                f"{symbol}: initialization error: "  
                f"{error}"  
            )  

        if (  
            completed % 100 == 0  
            or completed == len(symbols)  
        ):  

            elapsed = (  
                time.monotonic()  
                - start  
            )  

            print(  
                f"History progress: "  
                f"{completed}/{len(symbols)} | "  
                f"Ready: {successful} | "  
                f"Failed: {failed} | "  
                f"{elapsed:.1f}s"  
            )  

elapsed = (  
    time.monotonic()  
    - start  
)  

print()  

print(  
    f"Historical EMA data ready: "  
    f"{successful}/{len(symbols)}"  
)  

print(  
    f"Historical initialization time: "  
    f"{elapsed:.1f}s"  
)  

return successful

============================================================

MARKET STRUCTURE

============================================================

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

recent_high = float(  
    recent["high"].max()  
)  

previous_high = float(  
    previous["high"].max()  
)  

recent_low = float(  
    recent["low"].min()  
)  

previous_low = float(  
    previous["low"].min()  
)  

if (  
    recent_high > previous_high  
    and  
    recent_low > previous_low  
):  

    return "BULLISH"  

if (  
    recent_high < previous_high  
    and  
    recent_low < previous_low  
):  

    return "BEARISH"  

return "NEUTRAL"

============================================================

PERCENTAGE CHANGE

============================================================

def percentage_change(
old_value,
new_value
):

if old_value == 0:  

    return 0.0  

return (  
    (new_value - old_value)  
    / abs(old_value)  
) * 100

============================================================

DETERMINE MARKET CONDITION

============================================================

def get_market_condition(
df,
ema20,
ema50
):

if len(df) < TREND_LOOKBACK + 2:  

    return (  
        "INSUFFICIENT DATA",  
        "NEUTRAL",  
        False,  
        0.0,  
        0.0  
    )  

old_ema20 = float(  
    df["ema20"].iloc[  
        -TREND_LOOKBACK - 1  
    ]  
)  

old_ema50 = float(  
    df["ema50"].iloc[  
        -TREND_LOOKBACK - 1  
    ]  
)  

ema20_slope = percentage_change(  
    old_ema20,  
    ema20  
)  

ema50_slope = percentage_change(  
    old_ema50,  
    ema50  
)  

bullish_slope = (  
    ema20_slope  
    >= MIN_EMA20_SLOPE_PERCENT  
    and  
    ema50_slope  
    >= MIN_EMA50_SLOPE_PERCENT  
)  

bearish_slope = (  
    ema20_slope  
    <= -MIN_EMA20_SLOPE_PERCENT  
    and  
    ema50_slope  
    <= -MIN_EMA50_SLOPE_PERCENT  
)  

if bullish_slope:  

    return (  
        "BULLISH TREND",  
        "BULLISH",  
        True,  
        ema20_slope,  
        ema50_slope  
    )  

if bearish_slope:  

    return (  
        "BEARISH TREND",  
        "BEARISH",  
        True,  
        ema20_slope,  
        ema50_slope  
    )  

return (  
    "SIDEWAYS / RANGE",  
    "NEUTRAL",  
    False,  
    ema20_slope,  
    ema50_slope  
)

============================================================

ANALYZE SYMBOL

============================================================

def analyze_symbol(symbol):

with candles_lock:  

    history = candles.get(  
        symbol  
    )  

    if not history:  

        return None  

    df = pd.DataFrame(  
        list(history)  
    )  

if df.empty:  

    return None  

if len(df) < EMA_SLOW + 10:  

    return None  

df = df.sort_values(  
    "ts"  
).drop_duplicates(  
    subset=["ts"],  
    keep="last"  
).reset_index(  
    drop=True  
)  

# --------------------------------------------------------  
# EMA20  
# --------------------------------------------------------  

df["ema20"] = df[  
    "close"  
].ewm(  
    span=EMA_FAST,  
    adjust=False  
).mean()  

# --------------------------------------------------------  
# EMA50  
# --------------------------------------------------------  

df["ema50"] = df[  
    "close"  
].ewm(  
    span=EMA_SLOW,  
    adjust=False  
).mean()  

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
    / abs(current_ema50)  
) * 100  

# --------------------------------------------------------  
# ACTUAL GOLDEN CROSS  
# --------------------------------------------------------  

golden_cross = (  
    previous_ema20  
    <= previous_ema50  
    and  
    current_ema20  
    > current_ema50  
)  

# --------------------------------------------------------  
# ACTUAL DEATH CROSS  
# --------------------------------------------------------  

death_cross = (  
    previous_ema20  
    >= previous_ema50  
    and  
    current_ema20  
    < current_ema50  
)  

# --------------------------------------------------------  
# EMA MOVEMENT  
# --------------------------------------------------------  

if len(df) >= TREND_LOOKBACK + 2:  

    older_ema20 = float(  
        df["ema20"].iloc[  
            -TREND_LOOKBACK - 1  
        ]  
    )  

    older_ema50 = float(  
        df["ema50"].iloc[  
            -TREND_LOOKBACK - 1  
        ]  
    )  

else:  

    older_ema20 = previous_ema20  

    older_ema50 = previous_ema50  

ema20_slope = percentage_change(  
    older_ema20,  
    current_ema20  
)  

ema50_slope = percentage_change(  
    older_ema50,  
    current_ema50  
)  

# --------------------------------------------------------  
# IMPENDING GOLDEN CROSS  
# --------------------------------------------------------  

impending_golden = (  
    current_ema20  
    < current_ema50  
    and  
    ema_distance_percent  
    <= IMPENDING_CROSS_PERCENT  
    and  
    ema20_slope  
    > 0  
)  

# --------------------------------------------------------  
# IMPENDING DEATH CROSS  
# --------------------------------------------------------  

impending_death = (  
    current_ema20  
    > current_ema50  
    and  
    ema_distance_percent  
    <= IMPENDING_CROSS_PERCENT  
    and  
    ema20_slope  
    < 0  
)  

# --------------------------------------------------------  
# MARKET CONDITION  
# --------------------------------------------------------  

(  
    market_condition,  
    market_direction,  
    trending,  
    condition_ema20_slope,  
    condition_ema50_slope  
) = get_market_condition(  
    df,  
    current_ema20,  
    current_ema50  
)  

# --------------------------------------------------------  
# MARKET STRUCTURE  
# --------------------------------------------------------  

structure = get_market_structure(  
    df  
)  

# --------------------------------------------------------  
# TREND VALIDATION  
# --------------------------------------------------------  

bullish_trending_market = (  
    trending  
    and  
    market_direction == "BULLISH"  
    and  
    structure in (  
        "BULLISH",  
        "NEUTRAL"  
    )  
)  

bearish_trending_market = (  
    trending  
    and  
    market_direction == "BEARISH"  
    and  
    structure in (  
        "BEARISH",  
        "NEUTRAL"  
    )  
)  

# --------------------------------------------------------  
# SELECT SIGNAL  
# --------------------------------------------------------  

signal = None  

direction = None  

if golden_cross:  

    if bullish_trending_market:  

        signal = "GOLDEN CROSS"  

        direction = "BULLISH"  

elif death_cross:  

    if bearish_trending_market:  

        signal = "DEATH CROSS"  

        direction = "BEARISH"  

elif impending_golden:  

    if bullish_trending_market:  

        signal = "IMPENDING GOLDEN CROSS"  

        direction = "BULLISH"  

elif impending_death:  

    if bearish_trending_market:  

        signal = "IMPENDING DEATH CROSS"  

        direction = "BEARISH"  

# --------------------------------------------------------  
# NO VALID SIGNAL  
# --------------------------------------------------------  

if not signal:  

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

candle_timestamp = int(  
    current["ts"]  
)  

if candle_timestamp >= current_bucket:  

    candle_status = "FORMING"  

else:  

    candle_status = "CLOSED"  

# --------------------------------------------------------  
# FORMING CANDLE ONLY  
# --------------------------------------------------------  

if candle_status != "FORMING":  

    return None  

candle_time = datetime.fromtimestamp(  
    candle_timestamp / 1000,  
    tz=timezone.utc  
)  

if direction == "BULLISH":  

    trend_label = "BULLISH TREND"  

else:  

    trend_label = "BEARISH TREND"  

if trending:  

    market_status = market_condition  

else:  

    market_status = "SIDEWAYS / RANGE"  

return {  
    "symbol": symbol,  

    "signal": signal,  

    "direction": direction,  

    "status": candle_status,  

    "candle_time":  
        candle_time.strftime(  
            "%Y-%m-%d %H:%M UTC"  
        ),  

    "price": current_price,  

    "ema20": current_ema20,  

    "ema50": current_ema50,  

    "ema_distance":  
        ema_distance_percent,  

    "ema20_slope":  
        condition_ema20_slope,  

    "ema50_slope":  
        condition_ema50_slope,  

    "market_condition":  
        market_status,  

    "trend":  
        trend_label,  

    "structure":  
        structure  
}

============================================================

WEBSOCKET CANDLE HANDLER

============================================================

def handle_candle_message(message):

global ws_updates  
global ws_data_messages  
global ws_candle_messages  

if not message:  

    return  

if message == "pong":  

    return  

try:  

    data = json.loads(  
        message  
    )  

except (  
    json.JSONDecodeError,  
    TypeError  
):  

    return  

# --------------------------------------------------------  
# Count every JSON WebSocket message.  
# --------------------------------------------------------  

with ws_lock:  

    ws_data_messages += 1  

# --------------------------------------------------------  
# Bitget event messages  
# --------------------------------------------------------  

event = data.get(  
    "event"  
)  

if event:  

    if event == "subscribe":  

        with ws_lock:  

            ws_subscriptions += 1  

    elif event == "error":  

        print(  
            "WEBSOCKET SUBSCRIPTION ERROR:",  
            data  
        )  

    return  

arg = data.get(  
    "arg",  
    {}  
)  

channel = arg.get(  
    "channel",  
    ""  
)  

symbol = arg.get(  
    "instId",  
    ""  
)  

if not symbol:  

    return  

if channel != "candle30m":  

    return  

candle_data = data.get(  
    "data"  
)  

if not candle_data:  

    return  

with ws_lock:  

    ws_candle_messages += 1  

    ws_updates += len(  
        candle_data  
    )  

for row in candle_data:  

    if len(row) < 7:  

        continue  

    try:  

        candle = {  
            "ts": int(row[0]),  

            "open": float(row[1]),  

            "high": float(row[2]),  

            "low": float(row[3]),  

            "close": float(row[4]),  

            "volume": float(row[5]),  

            "quote_volume": float(row[6])  
        }  

    except (  
        ValueError,  
        TypeError  
    ):  

        continue  

    # ----------------------------------------------------  
    # UPDATE SHARED CANDLE  
    # ----------------------------------------------------  

    with candles_lock:  

        if symbol not in candles:  

            candles[symbol] = deque(  
                maxlen=CANDLE_LIMIT  
            )  

        history = candles[  
            symbol  
        ]  

        replaced = False  

        for index in range(  
            len(history) - 1,  
            -1,  
            -1  
        ):  

            existing = history[  
                index  
            ]  

            if int(  
                existing["ts"]  
            ) == candle["ts"]:  

                history[index] = candle  

                replaced = True  

                break  

        if not replaced:  

            history.append(  
                candle  
            )  

    # ----------------------------------------------------  
    # DIAGNOSTIC  
    # ----------------------------------------------------  

    current_update_count = 0  

    with ws_lock:  

        current_update_count = (  
            ws_updates  
        )  

    if (  
        current_update_count <= 20  
        or current_update_count % 100 == 0  
    ):  

        print(  
            f"WS DATA RECEIVED | "  
            f"{symbol} | "  
            f"candle30m | "  
            f"price={candle['close']:.8g}"  
        )  

    # ----------------------------------------------------  
    # ANALYZE IMMEDIATELY  
    # ----------------------------------------------------  

    result = analyze_symbol(  
        symbol  
    )  

    if result:  

        send_signal_if_new(  
            result  
        )

============================================================

SEND TELEGRAM SIGNAL IF NEW

============================================================

def send_signal_if_new(result):

symbol = result[  
    "symbol"  
]  

signal = result[  
    "signal"  
]  

candle_time = result[  
    "candle_time"  
]  

state_key = (  
    signal  
    + "|"  
    + candle_time  
)  

with alert_lock:  

    previous_state = (  
        last_alert_state.get(  
            symbol  
        )  
    )  

    if previous_state == state_key:  

        return  

    last_alert_state[  
        symbol  
    ] = state_key  

message = format_message(  
    result  
)  

if send_telegram(  
    message  
):  

    print(  
        f"TELEGRAM ALERT SENT | "  
        f"{symbol} | "  
        f"{signal}"  
    )  

else:  

    with alert_lock:  

        if (  
            last_alert_state.get(  
                symbol  
            )  
            == state_key  
        ):  

            last_alert_state.pop(  
                symbol,  
                None  
            )

============================================================

TELEGRAM

============================================================

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

============================================================

TELEGRAM MESSAGE

============================================================

def format_message(signal):

if signal["direction"] == "BULLISH":  

    emoji = "🟢"  

else:  

    emoji = "🔴"  

message = (  
    f"{emoji} {signal['signal']}\n\n"  

    f"Pair: {signal['symbol']}\n"  

    f"Exchange: Bitget Spot\n"  

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

    f"MARKET CONDITION\n"  
    f"{signal['market_condition']}\n\n"  

    f"TREND\n"  
    f"{signal['trend']}\n\n"  

    f"MARKET STRUCTURE\n"  
    f"{signal['structure']}\n\n"  

    f"EMA MOMENTUM\n"  
    f"EMA20 slope: "  
    f"{signal['ema20_slope']:.4f}%\n"  
    f"EMA50 slope: "  
    f"{signal['ema50_slope']:.4f}%\n\n"  

    f"FORMING CANDLE\n"  
    f"REAL-TIME / DEVELOPING"  
)  

return message

============================================================

WEBSOCKET CONNECTION WORKER

============================================================

def websocket_worker(
connection_number,
symbols
):

global ws_connections_live  

while True:  

    ws = None  

    try:  

        print(  
            f"WS {connection_number}: "  
            f"CONNECTING | "  
            f"{len(symbols)} pairs"  
        )  

        ws = websocket.WebSocketApp(  
            BITGET_WS_URL,  

            on_open=lambda socket:  
                websocket_on_open(  
                    socket,  
                    connection_number,  
                    symbols  
                ),  

            on_message=lambda socket,  
                message:  
                websocket_on_message(  
                    socket,  
                    connection_number,  
                    message  
                ),  

            on_error=lambda socket,  
                error:  
                websocket_on_error(  
                    socket,  
                    connection_number,  
                    error  
                ),  

            on_close=lambda socket,  
                close_status_code,  
                close_msg:  
                websocket_on_close(  
                    socket,  
                    connection_number,  
                    close_status_code,  
                    close_msg  
                )  
        )  

        ws.run_forever(  
            ping_interval=WS_PING_INTERVAL,  
            ping_timeout=10  
        )  

    except Exception as error:  

        print(  
            f"WS {connection_number}: "  
            f"ERROR | {error}"  
        )  

    with ws_lock:  

        ws_connections_live = max(  
            0,  
            ws_connections_live - 1  
        )  

    print(  
        f"WS {connection_number}: "  
        f"DISCONNECTED"  
    )  

    print(  
        f"WS {connection_number}: "  
        f"Reconnecting in "  
        f"{WS_RECONNECT_DELAY}s..."  
    )  

    time.sleep(  
        WS_RECONNECT_DELAY  
    )

============================================================

WEBSOCKET OPEN

============================================================

def websocket_on_open(
ws,
connection_number,
symbols
):

global ws_connections_live  

print(  
    f"WS {connection_number}: "  
    f"CONNECTED"  
)  

subscriptions = []  

for symbol in symbols:  

    subscriptions.append({  
        "instType": "SPOT",  
        "channel": "candle30m",  
        "instId": symbol  
    })  

request = {  
    "op": "subscribe",  
    "args": subscriptions  
}  

try:  

    ws.send(  
        json.dumps(  
            request  
        )  
    )  

    with ws_lock:  

        ws_connections_live += 1  

    print(  
        f"WS {connection_number}: "  
        f"SUBSCRIPTION REQUEST SENT | "  
        f"{len(symbols)} pairs"  
    )  

except Exception as error:  

    print(  
        f"WS {connection_number}: "  
        f"SUBSCRIBE ERROR | "  
        f"{error}"  
    )

============================================================

WEBSOCKET MESSAGE

============================================================

def websocket_on_message(
ws,
connection_number,
message
):

if message == "ping":  

    try:  

        ws.send(  
            "pong"  
        )  

    except Exception:  

        pass  

    return  

handle_candle_message(  
    message  
)

============================================================

WEBSOCKET ERROR

============================================================

def websocket_on_error(
ws,
connection_number,
error
):

print(  
    f"WS {connection_number}: "  
    f"ERROR | {error}"  
)

============================================================

WEBSOCKET CLOSE

============================================================

def websocket_on_close(
ws,
connection_number,
close_status_code,
close_msg
):

print(  
    f"WS {connection_number}: "  
    f"CLOSED | "  
    f"code={close_status_code} | "  
    f"message={close_msg}"  
)

============================================================

START WEBSOCKET CONNECTIONS

============================================================

def start_websocket_connections(
symbols
):

print()  
print("=" * 70)  
print(  
    "STARTING BITGET WEBSOCKET CONNECTIONS"  
)  
print("=" * 70)  

chunks = [  
    symbols[  
        i:i + WS_PAIRS_PER_CONNECTION  
    ]  
    for i in range(  
        0,  
        len(symbols),  
        WS_PAIRS_PER_CONNECTION  
    )  
]  

print(  
    f"Pairs: {len(symbols)}"  
)  

print(  
    f"Pairs per connection: "  
    f"{WS_PAIRS_PER_CONNECTION}"  
)  

print(  
    f"WebSocket connections required: "  
    f"{len(chunks)}"  
)  

print()  

threads = []  

for index, chunk in enumerate(  
    chunks,  
    start=1  
):  

    thread = threading.Thread(  
        target=websocket_worker,  
        args=(  
            index,  
            chunk  
        ),  
        daemon=True  
    )  

    thread.start()  

    threads.append(  
        thread  
    )  

    time.sleep(  
        0.15  
    )  

return threads

============================================================

WEBSOCKET DIAGNOSTIC

============================================================

def websocket_status_loop():

last_updates = 0  

while True:  

    time.sleep(  
        30  
    )  

    with ws_lock:  

        current_updates = (  
            ws_updates  
        )  

        data_messages = (  
            ws_data_messages  
        )  

        candle_messages = (  
            ws_candle_messages  
        )  

        live_connections = (  
            ws_connections_live  
        )  

    delta = (  
        current_updates  
        - last_updates  
    )  

    last_updates = (  
        current_updates  
    )  

    timestamp = datetime.now(  
        timezone.utc  
    ).strftime(  
        "%Y-%m-%d %H:%M:%S UTC"  
    )  

    print()  

    print(  
        f"{timestamp} | "  
        f"WebSocket status:"  
    )  

    print(  
        f"  Live connections: "  
        f"{live_connections}"  
    )  

    print(  
        f"  JSON messages received: "  
        f"{data_messages}"  
    )  

    print(  
        f"  Candle messages received: "  
        f"{candle_messages}"  
    )  

    print(  
        f"  Total candle updates: "  
        f"{current_updates}"  
    )  

    print(  
        f"  Updates in last 30 seconds: "  
        f"{delta}"  
    )  

    if current_updates > 0:  

        print(  
            "  FORMING CANDLE DATA: "  
            "CONFIRMED"  
        )  

    else:  

        print(  
            "  FORMING CANDLE DATA: "  
            "NOT YET RECEIVED"  
        )  

    print()

============================================================

MAIN

============================================================

def main():

print()  

print("=" * 70)  

print(  
    "BITGET EMA20 / EMA50 REAL-TIME "  
    "WEBSOCKET SCANNER"  
)  

print("=" * 70)  

print(  
    "Exchange: BITGET SPOT"  
)  

print(  
    "Pairs: ALL ONLINE USDT SPOT PAIRS"  
)  

print(  
    "Timeframe: 30 MINUTES"  
)  

print(  
    "EMA: 20 / 50"  
)  

print(  
    "Actual cross: ENABLED"  
)  

print(  
    "Impending cross: ENABLED"  
)  

print(  
    f"Impending threshold: "  
    f"{IMPENDING_CROSS_PERCENT}%"  
)  

print(  
    "Forming candle: ENABLED"  
)  

print(  
    "WebSocket: ENABLED"  
)  

print(  
    "Market condition: ENABLED"  
)  

print(  
    "Trend condition: ENABLED"  
)  

print(  
    "Market structure: ENABLED"  
)  

print(  
    "ADX: OFF"  
)  

print(  
    "VOLUME: OFF"  
)  

print(  
    "RSI: OFF"  
)  

print(  
    "BOLLINGER BANDS: OFF"  
)  

print(  
    "SQUEEZE: OFF"  
)  

print(  
    "SIDEWAYS SIGNALS: BLOCKED"  
)  

print("=" * 70)  

print()  

# --------------------------------------------------------  
# GET SYMBOLS  
# --------------------------------------------------------  

symbols = get_symbols()  

if not symbols:  

    raise RuntimeError(  
        "No Bitget USDT Spot symbols found."  
    )  

# --------------------------------------------------------  
# INITIALIZE HISTORY  
# --------------------------------------------------------  

initialize_history(  
    symbols  
)  

# --------------------------------------------------------  
# START WEBSOCKET  
# --------------------------------------------------------  

start_websocket_connections(  
    symbols  
)  

# --------------------------------------------------------  
# START DIAGNOSTIC THREAD  
# --------------------------------------------------------  

diagnostic_thread = threading.Thread(  
    target=websocket_status_loop,  
    daemon=True  
)  

diagnostic_thread.start()  

# --------------------------------------------------------  
# SCANNER STATUS  
# --------------------------------------------------------  

print()  

print("=" * 70)  

print(  
    "WEBSOCKET SCANNER IS NOW LIVE"  
)  

print(  
    f"Pairs: {len(symbols)}"  
)  

print(  
    f"WebSocket connections: "  
    f"{(  
        len(symbols)  
        + WS_PAIRS_PER_CONNECTION  
        - 1  
    ) // WS_PAIRS_PER_CONNECTION}"  
)  

print(  
    "Waiting for FORMING 30-minute "  
    "candle updates..."  
)  

print("=" * 70)  

print()  

# --------------------------------------------------------  
# KEEP PROCESS ALIVE  
# --------------------------------------------------------  

while True:  

    time.sleep(  
        60  
    )

============================================================

START

============================================================

if name == "main":

try:  

    main()  

except KeyboardInterrupt:  

    print(  
        "Scanner stopped."  
    )  

except Exception as error:  

    print(  
        f"MAIN ERROR: {error}"  
    )  

    raise
