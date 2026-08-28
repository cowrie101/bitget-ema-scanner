import os
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd


# ============================================================
# BITGET EMA 20/50 - FAST SCANNER v3
# 30-MINUTE FORMING + CLOSED CANDLE
# EMA + VOLUME + ADX + MARKET STRUCTURE + RSI
# ============================================================

BITGET_SYMBOLS = "https://api.bitget.com/api/v2/spot/public/symbols"
BITGET_CANDLES = "https://api.bitget.com/api/v2/spot/market/candles"
TELEGRAM_SEND = "https://api.telegram.org/bot{}/sendMessage"

EMA_FAST = 20
EMA_SLOW = 50
GRANULARITY = "30min"
CANDLE_LIMIT = 200

MAX_WORKERS = 10
EMA_APPROACH_PERCENT = 0.20
ADX_MIN = 20
VOLUME_LOOKBACK = 20
VOLUME_MULTIPLIER = 1.20

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

session = requests.Session()
session.headers.update({"User-Agent": "Bitget-EMA-Scanner/3.0"})
# ============================================================
# GET ALL ONLINE USDT SPOT PAIRS
# ============================================================

def get_symbols():
    response = session.get(BITGET_SYMBOLS, timeout=20)
    response.raise_for_status()
    data = response.json()

    if data.get("code") != "00000":
        raise RuntimeError(f"Bitget symbols error: {data}")

    return sorted(set(
        item.get("symbol", "")
        for item in data.get("data", [])
        if item.get("quoteCoin") == "USDT"
        and item.get("status") == "online"
        and item.get("symbol", "").endswith("USDT")
    ))


# ============================================================
# GET 30-MINUTE CANDLES
# ============================================================

def get_candles(symbol):
    params = {
        "symbol": symbol,
        "granularity": GRANULARITY,
        "limit": str(CANDLE_LIMIT),
    }

    for attempt in range(3):
        try:
            response = session.get(
                BITGET_CANDLES,
                params=params,
                timeout=15
            )

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue

            if response.status_code == 400:
                return None

            response.raise_for_status()
            data = response.json()

            if data.get("code") != "00000":
                return None

            rows = data.get("data", [])

            if len(rows) < EMA_SLOW + 5:
                return None

            return rows

        except requests.RequestException:
            if attempt == 2:
                return None

            time.sleep(1.5 * (attempt + 1))

    return None


# ============================================================
# CREATE DATAFRAME
# ============================================================

def make_dataframe(rows):
    cleaned_rows = [
        row[:7] for row in rows
        if len(row) >= 7
    ]

    cleaned_rows.sort(
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

    for column in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_volume"
    ]:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    df["ts"] = pd.to_numeric(
        df["ts"],
        errors="coerce"
    )

    return df.dropna(
        subset=[
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )
# ============================================================
# INDICATORS
# ============================================================

def add_indicators(df):

    # EMA 20
    df["ema20"] = df["close"].ewm(
        span=EMA_FAST,
        adjust=False
    ).mean()

    # EMA 50
    df["ema50"] = df["close"].ewm(
        span=EMA_SLOW,
        adjust=False
    ).mean()


    # ========================================================
    # RSI 14
    # ========================================================

    delta = df["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / 14,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / 14,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        pd.NA
    )

    df["rsi"] = (
        100 - (100 / (1 + rs))
    ).fillna(50)


    # ========================================================
    # ADX 14
    # ========================================================

    high = df["high"]
    low = df["low"]
    close = df["close"]

    previous_close = close.shift(1)

    true_range = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where(
        (up_move > down_move)
        & (up_move > 0),
        0
    )

    minus_dm = down_move.where(
        (down_move > up_move)
        & (down_move > 0),
        0
    )

    atr = true_range.ewm(
        alpha=1 / 14,
        adjust=False
    ).mean()

    plus_di = (
        100
        * plus_dm.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()
        / atr.replace(0, pd.NA)
    )

    minus_di = (
        100
        * minus_dm.ewm(
            alpha=1 / 14,
            adjust=False
        ).mean()
        / atr.replace(0, pd.NA)
    )

    dx = (
        100
        * (plus_di - minus_di).abs()
        / (plus_di + minus_di).replace(
            0,
            pd.NA
        )
    )

    df["adx"] = dx.ewm(
        alpha=1 / 14,
        adjust=False
    ).mean().fillna(0)

    df["plus_di"] = plus_di.fillna(0)
    df["minus_di"] = minus_di.fillna(0)


    # ========================================================
    # VOLUME
    # ========================================================

    df["volume_avg"] = df["volume"].rolling(
        VOLUME_LOOKBACK
    ).mean()


    # ========================================================
    # MARKET STRUCTURE
    # ========================================================

    df["recent_high"] = df["high"].rolling(
        5
    ).max()

    df["previous_high"] = df["high"].shift(
        5
    ).rolling(
        5
    ).max()

    df["recent_low"] = df["low"].rolling(
        5
    ).min()

    df["previous_low"] = df["low"].shift(
        5
    ).rolling(
        5
    ).min()

    return df
# ============================================================
# ANALYZE ONE SYMBOL
# ============================================================

def analyze_symbol(symbol):

    rows = get_candles(symbol)

    if not rows:
        return None

    df = make_dataframe(rows)

    if len(df) < EMA_SLOW + 20:
        return None

    df = add_indicators(df)

    previous = df.iloc[-2]
    current = df.iloc[-1]

    ema20 = float(current["ema20"])
    ema50 = float(current["ema50"])

    if ema50 == 0:
        return None

    # Distance between EMA20 and EMA50
    distance = (
        abs(ema20 - ema50)
        / abs(ema50)
        * 100
    )

    # ========================================================
    # ACTUAL CROSS
    # ========================================================

    golden = (
        previous["ema20"] <= previous["ema50"]
        and current["ema20"] > current["ema50"]
    )

    death = (
        previous["ema20"] >= previous["ema50"]
        and current["ema20"] < current["ema50"]
    )

    # ========================================================
    # EARLY CROSSOVER WARNING
    # ========================================================

    early_golden = (
        not golden
        and ema20 < ema50
        and distance <= EMA_APPROACH_PERCENT
        and current["close"] >= ema20
    )

    early_death = (
        not death
        and ema20 > ema50
        and distance <= EMA_APPROACH_PERCENT
        and current["close"] <= ema20
    )

    # ========================================================
    # DETERMINE SIGNAL
    # ========================================================

    if golden:

        direction = "GOLDEN CROSS"
        early = False

    elif death:

        direction = "DEATH CROSS"
        early = False

    elif early_golden:

        direction = "GOLDEN CROSS APPROACHING"
        early = True

    elif early_death:

        direction = "DEATH CROSS APPROACHING"
        early = True

    else:

        return None


    # ========================================================
    # VOLUME
    # ========================================================

    volume_avg = current["volume_avg"]

    if (
        pd.notna(volume_avg)
        and volume_avg > 0
    ):

        volume_ratio = float(
            current["volume"]
            / volume_avg
        )

    else:

        volume_ratio = 0


    volume_confirmed = (
        volume_ratio >= VOLUME_MULTIPLIER
    )


    # ========================================================
    # ADX
    # ========================================================

    adx = float(
        current["adx"]
    )

    adx_confirmed = (
        adx >= ADX_MIN
    )


    # ========================================================
    # RSI
    # ========================================================

    rsi = float(
        current["rsi"]
    )


    # ========================================================
    # MARKET STRUCTURE
    # ========================================================

    if "GOLDEN" in direction:

        bullish_structure = (
            current["recent_high"]
            > current["previous_high"]
            and
            current["recent_low"]
            > current["previous_low"]
        )

        structure = (
            "BULLISH"
            if bullish_structure
            else "MIXED"
        )

        di_confirmed = (
            current["plus_di"]
            > current["minus_di"]
        )

        rsi_confirmed = (
            rsi >= 50
        )

    else:

        bearish_structure = (
            current["recent_high"]
            < current["previous_high"]
            and
            current["recent_low"]
            < current["previous_low"]
        )

        structure = (
            "BEARISH"
            if bearish_structure
            else "MIXED"
        )

        di_confirmed = (
            current["minus_di"]
            > current["plus_di"]
        )

        rsi_confirmed = (
            rsi <= 50
        )


    # ========================================================
    # SIGNAL SCORE
    # ========================================================

    score = 0

    if volume_confirmed:
        score += 1

    if adx_confirmed:
        score += 1

    if structure in (
        "BULLISH",
        "BEARISH"
    ):
        score += 1

    if rsi_confirmed:
        score += 1

    if di_confirmed:
        score += 1


    # ========================================================
    # SIGNAL GRADE
    # ========================================================

    if score >= 4:

        grade = "A - STRONG"

    elif score >= 2:

        grade = "B - DEVELOPING"

    else:

        grade = "C - EARLY / WEAK"


    # ========================================================
    # CANDLE STATUS
    # ========================================================

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

        status = "INTRABAR"

    else:

        status = "CANDLE CLOSED"


    # ========================================================
    # CANDLE TIME
    # ========================================================

    candle_time = datetime.fromtimestamp(
        candle_timestamp / 1000,
        tz=timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


    # ========================================================
    # RETURN SIGNAL
    # ========================================================

    return {

        "symbol": symbol,

        "direction": direction,

        "status": status,

        "early": early,

        "candle_time": candle_time,

        "price": float(
            current["close"]
        ),

        "ema20": ema20,

        "ema50": ema50,

        "distance": distance,

        "volume_ratio": volume_ratio,

        "adx": adx,

        "rsi": rsi,

        "structure": structure,

        "score": score,

        "grade": grade
    }
# ============================================================
# SEND TELEGRAM MESSAGE
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
            timeout=20
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

    except requests.RequestException as e:

        print(
            "Telegram error:",
            e
        )

        return False


# ============================================================
# BUILD TELEGRAM MESSAGE
# ============================================================

def build_message(signal):

    if "GOLDEN" in signal["direction"]:

        emoji = "🟢"

    else:

        emoji = "🔴"


    if signal["early"]:

        title = (
            f"{emoji} EARLY "
            f"{signal['direction']}"
        )

    else:

        title = (
            f"{emoji} "
            f"{signal['direction']}"
        )


    return (
        f"{title}\n\n"

        f"Pair: "
        f"{signal['symbol']}\n"

        f"Timeframe: "
        f"30 MINUTES\n"

        f"Status: "
        f"{signal['status']}\n"

        f"Candle: "
        f"{signal['candle_time']}\n\n"

        f"Price: "
        f"{signal['price']:.8g}\n"

        f"EMA20: "
        f"{signal['ema20']:.8g}\n"

        f"EMA50: "
        f"{signal['ema50']:.8g}\n"

        f"EMA distance: "
        f"{signal['distance']:.3f}%\n\n"

        f"Volume: "
        f"{signal['volume_ratio']:.2f}x average\n"

        f"ADX: "
        f"{signal['adx']:.1f}\n"

        f"RSI: "
        f"{signal['rsi']:.1f}\n"

        f"Structure: "
        f"{signal['structure']}\n\n"

        f"Setup grade: "
        f"{signal['grade']}\n\n"

        f"Bitget Spot"
    )
# ============================================================
# MAIN SCANNER
# ============================================================

def main():

    print("=" * 65)

    print(
        "BITGET EMA20 / EMA50 FAST 30-MINUTE SCANNER v3"
    )

    print(
        "EARLY + INTRABAR + CLOSED CANDLE MODE"
    )

    print(
        "EMA + VOLUME + ADX + MARKET STRUCTURE + RSI"
    )

    print("=" * 65)

    start = time.time()

    symbols = get_symbols()

    print(
        f"Scanning {len(symbols)} "
        f"online USDT Spot pairs "
        f"with {MAX_WORKERS} workers..."
    )

    signals = []

    # --------------------------------------------------------
    # SCAN MANY PAIRS AT THE SAME TIME
    # --------------------------------------------------------

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        futures = {
            executor.submit(
                analyze_symbol,
                symbol
            ): symbol
            for symbol in symbols
        }

        completed = 0

        for future in as_completed(
            futures
        ):

            completed += 1

            symbol = futures[future]

            try:

                result = future.result()

                if result:

                    signals.append(
                        result
                    )

                    print(
                        f"{result['direction']} | "
                        f"{result['status']} | "
                        f"{symbol} | "
                        f"Grade "
                        f"{result['grade']}"
                    )

            except Exception as e:

                print(
                    f"{symbol}: {e}"
                )

            if completed % 100 == 0:

                print(
                    f"Progress: "
                    f"{completed}/"
                    f"{len(symbols)}"
                )


    # --------------------------------------------------------
    # SORT STRONGEST SIGNALS FIRST
    # --------------------------------------------------------

    signals.sort(
        key=lambda x: (
            -x["score"],
            x["symbol"]
        )
    )


    scan_time = (
        time.time() - start
    )

    print(
        f"Scan completed in "
        f"{scan_time:.1f} seconds."
    )


    # --------------------------------------------------------
    # SEND TELEGRAM ALERTS
    # --------------------------------------------------------

    if signals:

        for signal in signals:

            message = build_message(
                signal
            )

            if send_telegram(
                message
            ):

                print(
                    "Telegram alert sent:",
                    signal["symbol"]
                )

    else:

        print(
            "No EMA20/EMA50 developing "
            "or confirmed signals detected "
            "during this scan."
        )


    print()

    print(
        "SCAN COMPLETE"
    )


# ============================================================
# START PROGRAM
# ============================================================

if __name__ == "__main__":

    main()
