import os
import time
from datetime import datetime, timezone
import requests
import pandas as pd

BITGET_SYMBOLS = "https://api.bitget.com/api/v2/spot/public/symbols"
BITGET_CANDLES = "https://api.bitget.com/api/v2/spot/market/history-candles"
TELEGRAM_SEND = "https://api.telegram.org/bot{}/sendMessage"

EMA_FAST = 20
EMA_SLOW = 50
GRANULARITY = "30min"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

session = requests.Session()
session.headers.update({"User-Agent": "Bitget-EMA-Scanner/1.0"})

def get_symbols():
    r = session.get(BITGET_SYMBOLS, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != "00000":
        raise RuntimeError(f"Bitget symbols error: {data}")
    return [
        x["symbol"] for x in data["data"]
        if x.get("quoteCoin") == "USDT" and x.get("status") == "online"
    ]

def get_candles(symbol):
    params = {"symbol": symbol, "granularity": GRANULARITY, "limit": 200}
    r = session.get(BITGET_CANDLES, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("code") != "00000":
        return []
    rows = data.get("data", [])
    if len(rows) < 60:
        return []
    # Bitget returns newest candles first. Sort oldest -> newest.
    rows = sorted(rows, key=lambda x: int(x[0]))
    df = pd.DataFrame(rows, columns=["ts","open","high","low","close","volume","quote_volume"])
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce")
    df = df.dropna(subset=["close"])
    # Ignore the currently forming 30-minute candle.
    now_ms = int(time.time() * 1000)
    interval_ms = 30 * 60 * 1000
    current_bucket = (now_ms // interval_ms) * interval_ms
    df = df[df["ts"] < current_bucket]
    return df

def crossover(symbol):
    df = get_candles(symbol)
    if len(df) < 55:
        return None
    df["ema20"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema50"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
    a = df.iloc[-2]
    b = df.iloc[-1]
    if a["ema20"] <= a["ema50"] and b["ema20"] > b["ema50"]:
        return ("GOLDEN CROSS", b)
    if a["ema20"] >= a["ema50"] and b["ema20"] < b["ema50"]:
        return ("DEATH CROSS", b)
    return None

def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram secrets are not set. Signal was:", text)
        return
    url = TELEGRAM_SEND.format(TELEGRAM_BOT_TOKEN)
    r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=20)
    r.raise_for_status()

def main():
    symbols = get_symbols()
    print(f"Scanning {len(symbols)} online USDT Spot pairs...")
    signals = []
    for i, symbol in enumerate(symbols, 1):
        try:
            result = crossover(symbol)
            if result:
                side, candle = result
                signals.append(
                    f"{'🟢' if side == 'GOLDEN CROSS' else '🔴'} {side}\n"
                    f"{symbol} | 30M\n"
                    f"EMA20: {candle['ema20']:.8g}\n"
                    f"EMA50: {candle['ema50']:.8g}\n"
                    f"Close: {candle['close']:.8g}"
                )
        except Exception as e:
            print(f"{symbol}: {e}")
        # Stay comfortably below Bitget's 20 requests/sec endpoint limit.
        time.sleep(0.06)

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if signals:
        message = f"BITGET EMA 20/50 SCANNER\n{stamp}\n\n" + "\n\n".join(signals)
        send_telegram(message)
        print(message)
    else:
        print(f"No new EMA20/EMA50 crossover on the latest closed 30M candle. {stamp}")

if __name__ == "__main__":
    main()
