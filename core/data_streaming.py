# core/data_streaming.py (배치 저장 + 커넥션 재사용 개선)

import asyncio
import json
from datetime import datetime, timedelta, timezone
import psycopg2
import os
from dotenv import load_dotenv
import pandas as pd
from api.api_websocket import WebSocketManager
from api.api_quotation import get_krw_tickers
import websockets

# 환경 변수 로드
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")
load_dotenv(dotenv_path=ENV_PATH)

# PostgreSQL 연결 정보
PG_CONFIG = {
    'dbname': os.getenv("PG_DBNAME"),
    'user': os.getenv("PG_USER"),
    'password': os.getenv("PG_PASSWORD"),
    'host': os.getenv("PG_HOST"),
    'port': int(os.getenv("PG_PORT", 5432))
}

# 안전한 값 처리
def safe(val):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return 0.0
    return float(val)

# 데이터 큐 (비동기 안전)
data_queue = asyncio.Queue()

# PostgreSQL 배치 저장 함수
async def save_batch_to_db():
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    try:
        while True:
            batch = []
            while len(batch) < 200:
                try:
                    row = await asyncio.wait_for(data_queue.get(), timeout=0.1)
                    batch.append(row)
                except asyncio.TimeoutError:
                    break

            if batch:
                try:
                    cur.executemany("""
                        INSERT INTO data_realtime (
                            trade_timestamp, ticker, trade_price, trade_volume, ask_bid,
                            best_ask_price, best_ask_size, best_bid_price, best_bid_size
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                    """, batch)
                    conn.commit()
                except Exception as e:
                    print(f"[DB ERROR] Failed batch insert: {e}")
                    conn.rollback()
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        cur.close()
        conn.close()
        print("[Shutdown] DB connection closed.")

# 오래된 데이터 삭제 (30분 초과)
def delete_old_data():
    try:
        with psycopg2.connect(**PG_CONFIG) as conn:
            with conn.cursor() as cur:
                cutoff = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
                cur.execute("DELETE FROM data_realtime WHERE trade_timestamp < %s", (cutoff,))
    except Exception as e:
        print(f"[DB ERROR] Failed to delete old data: {e}")

# 메시지 처리 → 큐로 전달
async def handle_message(data):
    try:
        row = (
            int(data.get("trade_timestamp")),
            data.get("code"),
            safe(data.get("trade_price")),
            safe(data.get("trade_volume")),
            data.get("ask_bid"),
            safe(data.get("best_ask_price")),
            safe(data.get("best_ask_size")),
            safe(data.get("best_bid_price")),
            safe(data.get("best_bid_size"))
        )
        await data_queue.put(row)
    except Exception as e:
        print(f"[HANDLE ERROR] {e}")

# 메인 루프
async def main():
    tickers = get_krw_tickers()
    ws = WebSocketManager(type="trade", codes=tickers)
    await ws.connect()

    async def recv_loop():
        while True:
            try:
                data = await ws.receive()
                if data:
                    await handle_message(data)
            except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
                print(f"[WebSocketManager] Disconnected: {e}. Reconnecting...")
                await ws.connect()
            except Exception as e:
                print(f"[WebSocketManager] Unexpected error: {e}")
                await asyncio.sleep(1)

    async def cleanup_loop():
        while True:
            delete_old_data()
            await asyncio.sleep(60)

    try:
        await asyncio.gather(recv_loop(), save_batch_to_db(), cleanup_loop())
    except asyncio.CancelledError:
        print("[Shutdown] Async task cancelled. Cleaning up...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[Exit] Stopped by user")
