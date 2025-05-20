# workers/sorting_worker.py (prediction_queue 제거됨, data_translate_1s에만 저장)

import asyncio
import os
import pandas as pd
import numpy as np
import sqlalchemy
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from api.api_quotation import get_krw_tickers  # 전체 KRW 티커 불러오기

# 환경 변수 로드
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

PG_CONFIG = {
    'dbname': os.getenv("PG_DBNAME"),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
    'host': os.getenv("PG_HOST"),
    'port': int(os.getenv("PG_PORT", 5432))
}

DB_URL = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
engine = sqlalchemy.create_engine(DB_URL)

def fetch_data_for_second(target_sec):
    query = f"""
        SELECT * FROM data_realtime
        WHERE trade_timestamp >= {target_sec * 1000} AND trade_timestamp < {(target_sec + 1) * 1000}
    """
    df = pd.read_sql(query, engine)
    return df

def convert_float(val):
    try:
        return float(val)
    except Exception:
        return None

def insert_ohlcv_batch(data):
    if not data:
        return
    with engine.connect() as conn:
        with conn.begin():
            for row in data:
                try:
                    row_dict = {
                        "time": row[0],
                        "ticker": row[1],
                        "open": convert_float(row[2]),
                        "high": convert_float(row[3]),
                        "low": convert_float(row[4]),
                        "close": convert_float(row[5]),
                        "volume": convert_float(row[6]),
                        "ask_bid": row[7],
                        "best_ask_price": convert_float(row[8]),
                        "best_ask_size": convert_float(row[9]),
                        "best_bid_price": convert_float(row[10]),
                        "best_bid_size": convert_float(row[11])
                    }
                    conn.execute(sqlalchemy.text("""
                        INSERT INTO data_translate_1s (
                            time, ticker, open, high, low, close, volume,
                            ask_bid, best_ask_price, best_ask_size, best_bid_price, best_bid_size
                        ) VALUES (:time, :ticker, :open, :high, :low, :close, :volume,
                                 :ask_bid, :best_ask_price, :best_ask_size, :best_bid_price, :best_bid_size)
                        ON CONFLICT DO NOTHING
                    """), row_dict)

                except Exception as e:
                    print(f"[DB ERROR] Failed to insert row: {e}")

def delete_old_data():
    cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=60)
    with engine.connect() as conn:
        with conn.begin():
            try:
                conn.execute(sqlalchemy.text("DELETE FROM data_translate_1s WHERE time < :cutoff"), {"cutoff": cutoff_time})
            except Exception as e:
                print(f"[DB ERROR] Failed to delete old rows: {e}")

async def run():
    latest_sec = int(datetime.now(tz=timezone.utc).timestamp()) - 2
    tickers = get_krw_tickers()
    cache = {}
    delete_timer = 0

    while True:
        now_sec = int(datetime.now(tz=timezone.utc).timestamp())
        if latest_sec >= now_sec - 1:
            await asyncio.sleep(0.5)
            continue

        df = fetch_data_for_second(latest_sec)
        result = []

        for ticker in tickers:
            if ticker not in cache:
                cache[ticker] = (0.0, '', 0.0, 0.0, 0.0, 0.0)

        if not df.empty:
            df_group = df.groupby("ticker")
            for ticker, group in df_group:
                o = group.sort_values("trade_timestamp")
                open_ = o["trade_price"].iloc[0]
                high = o["trade_price"].max()
                low = o["trade_price"].min()
                close = o["trade_price"].iloc[-1]
                volume = o["trade_volume"].sum()
                ask_bid = o["ask_bid"].iloc[-1]
                best_ask_price = o["best_ask_price"].iloc[-1]
                best_ask_size = o["best_ask_size"].iloc[-1]
                best_bid_price = o["best_bid_price"].iloc[-1]
                best_bid_size = o["best_bid_size"].iloc[-1]
                cache[ticker] = (close, ask_bid, best_ask_price, best_ask_size, best_bid_price, best_bid_size)

                result.append([
                    datetime.fromtimestamp(latest_sec, tz=timezone.utc), ticker,
                    open_, high, low, close, volume,
                    ask_bid, best_ask_price, best_ask_size, best_bid_price, best_bid_size
                ])

        for ticker in tickers:
            if not df.empty and ticker in df["ticker"].values:
                continue
            close, ask_bid, best_ask_price, best_ask_size, best_bid_price, best_bid_size = cache[ticker]
            result.append([
                datetime.fromtimestamp(latest_sec, tz=timezone.utc), ticker,
                close, close, close, close, 0.0,
                ask_bid, best_ask_price, best_ask_size, best_bid_price, best_bid_size
            ])

        insert_ohlcv_batch(result)
        latest_sec += 1

        delete_timer += 1
        if delete_timer >= 60:
            delete_old_data()
            delete_timer = 0

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[Exit] Sorting worker stopped by user.")
