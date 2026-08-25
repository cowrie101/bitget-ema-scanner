import os
import time
from datetime import datetime, timezone

import requests
import pandas as pd


# ============================================================
# SETTINGS
# ============================================================

BITGET_SYMBOLS = "https://api.bitget.com/api/v2/spot/public/symbols"
BITGET_CANDLES = "https://api.bitget.com/api/v2/spot/market/history-candles"
TELEGRAM_SEND = "https://api.telegram.org/bot{}/sendMessage"

EMA_FAST = 20
EMA_SLOW = 50
GRANULARITY = "30min"

CANDLE_LIMIT = 200

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")


# ============================================================
# SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Bitget-EMA-Scanner/2.0"
})


# ============================================================
# GET ALL ONLINE USDT SPOT PAIRS
# ============================================================

def get_symbols():

    response = session.get(
        BITGET_SYMBOLS,
        timeout=20
    )

    response.raise_for_status()

    data = response.json()

    if data.get("code") != "00000":
        raise RuntimeError(
            f"Bitget symbols error: {data}"
        )

    symbols = []

    for item in data.get("data", []):

        symbol = item.get("symbol", "")
        quote_coin = item.get("quoteCoin", "")
        status = item.get("status", "")

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

    # Bitget requires an endTime for this request.
    end_time = int(time.time() * 1000)

    params = {
        "symbol": symbol,
        "granularity": GRANULARITY,
        "endTime": str(end_time),
        "limit": str(CANDLE_LIMIT)
    }

    for attempt in range(3):

        try:

            response = session.get(
                BITGET_CANDLES,
                params=params,
                timeout=20
            )

            # Temporary rate/server problems
            if response.status_code in (
                429, 500, 502, 503, 504
            ):

                time.sleep(2 + attempt)

                continue

            # Invalid/unavailable symbol
            if response.status_code == 400:

                return None

            response.raise_for_status()

            data = response.json()

            if data.get("code") != "00000":
                return None

            rows = data.get("data", [])

            if len(rows) < EMA_SLOW:
                return None

            return rows

        except requests.RequestException as e:

            if attempt == 2:

                print(
                    f"{symbol}: request failed: {e}"
                )

                return None

            time.sleep(2 + attempt)

    return None


# ============================================================
# CREATE DATAFRAME
# ============================================================
def make_dataframe(rows):

    cleaned_rows = []

    for row in rows:
        if len(row) >= 7:
            cleaned_rows.append(row[:7])

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

    df = df.dropna(
        subset=["ts", "close"]
    )

    return df
# ============================================================
# CALCULATE CROSS
#
# We deliberately keep the CURRENT FORMING candle.
# ============================================================

def check_crossover(symbol):

    rows = get_candles(symbol)

    if not rows:
        return None

    df = make_dataframe(rows)

    if len(df) < EMA_SLOW + 2:
        return None

    # EMA calculations include the current forming candle.
    df["ema20"] = df["close"].ewm(
        span=EMA_FAST,
        adjust=False
    ).mean()

    df["ema50"] = df["close"].ewm(
        span=EMA_SLOW,
        adjust=False
    ).mean()

    previous = df.iloc[-2]
    current = df.iloc[-1]

    previous_fast = previous["ema20"]
    previous_slow = previous["ema50"]

    current_fast = current["ema20"]
    current_slow = current["ema50"]

    signal = None

    # --------------------------------------------------------
    # GOLDEN CROSS
    # --------------------------------------------------------

    if (
        previous_fast <= previous_slow
        and current_fast > current_slow
    ):

        signal = "GOLDEN CROSS"

    # --------------------------------------------------------
    # DEATH CROSS
    # --------------------------------------------------------

    elif (
        previous_fast >= previous_slow
        and current_fast < current_slow
    ):

        signal = "DEATH CROSS"

    if signal is None:
        return None

    # Determine whether current candle is still forming.
    interval_ms = 30 * 60 * 1000

    current_bucket = (
        int(time.time() * 1000)
        // interval_ms
    ) * interval_ms

    candle_timestamp = int(
        current["ts"]
    )

    if candle_timestamp >= current_bucket:

        candle_status = "INTRABAR"

    else:

        candle_status = "CANDLE CLOSED"

    candle_time = datetime.fromtimestamp(
        candle_timestamp / 1000,
        tz=timezone.utc
    )

    return {
        "symbol": symbol,
        "signal": signal,
        "status": candle_status,
        "candle_time": candle_time.strftime(
            "%Y-%m-%d %H:%M UTC"
        ),
        "price": current["close"],
        "ema20": current_fast,
        "ema50": current_slow
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
# MAIN
# ============================================================

def main():

    print("=" * 60)

    print(
        "BITGET EMA20 / EMA50 30-MINUTE SCANNER"
    )

    print(
        "FORMING + CLOSED CANDLE MODE"
    )

    print("=" * 60)

    stamp = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print("Scan started:", stamp)

    symbols = get_symbols()

    print(
        f"Scanning {len(symbols)} "
        f"online USDT Spot pairs..."
    )

    print()

    signals = []

    for number, symbol in enumerate(
        symbols,
        1
    ):

        try:

            result = check_crossover(
                symbol
            )

            if result:

                signals.append(result)

                print(
                    f"{result['signal']} | "
                    f"{result['status']} | "
                    f"{symbol}"
                )

        except Exception as e:

            print(
                f"{symbol}: {e}"
            )

        # Stay below Bitget request limits.
        time.sleep(0.06)

        if number % 100 == 0:

            print(
                f"Progress: "
                f"{number}/{len(symbols)}"
            )

    # ========================================================
    # SEND SIGNALS
    # ========================================================

    if signals:

        for signal in signals:

            if signal["signal"] == "GOLDEN CROSS":

                emoji = "🟢"

            else:

                emoji = "🔴"

            message = (
                f"{emoji} "
                f"{signal['signal']}\n\n"

                f"Pair: {signal['symbol']}\n"

                f"Timeframe: 30 MINUTES\n"

                f"Status: {signal['status']}\n"

                f"Candle: "
                f"{signal['candle_time']}\n\n"

                f"Price: "
                f"{signal['price']:.8g}\n"

                f"EMA20: "
                f"{signal['ema20']:.8g}\n"

                f"EMA50: "
                f"{signal['ema50']:.8g}\n\n"

                f"Bitget Spot"
            )

            if send_telegram(message):

                print(
                    "Telegram alert sent:",
                    signal["symbol"]
                )

    else:

        print(
            "No new EMA20/EMA50 crossover "
            "detected during this scan."
        )

    print()

    print(
        "SCAN COMPLETE"
    )


if __name__ == "__main__":

    main()
