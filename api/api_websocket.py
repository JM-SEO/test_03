# api/api_websocket.py (최종 안정성 개선 버전)
import websockets
import asyncio
import json
import uuid
import multiprocessing as mp


class WebSocketClient:
    def __init__(self, type: str, codes: list, queue: mp.Queue):
        self.type = type
        self.codes = codes
        self.queue = queue

    async def connect_socket(self):
        uri = "wss://api.upbit.com/websocket/v1"
        async for websocket in websockets.connect(
            uri,
            ping_interval=30,
            ping_timeout=60,
            max_queue=4096,
            close_timeout=3,
            max_size=None
        ):
            try:
                data = [
                    {"ticket": str(uuid.uuid4())[:6]},
                    {
                        "type": self.type,
                        "codes": self.codes,
                        "isOnlyRealtime": True
                    }
                ]
                await websocket.send(json.dumps(data))

                while True:
                    recv_data = await asyncio.wait_for(websocket.recv(), timeout=30)
                    recv_data = recv_data.decode('utf-8')
                    self.queue.put(json.loads(recv_data))
            except (websockets.ConnectionClosed, asyncio.TimeoutError) as e:
                self.queue.put({"error": f"ConnectionClosed: {e}"})
                await asyncio.sleep(3)
                continue
            except Exception as e:
                self.queue.put({"error": f"Unexpected error: {e}"})
                await asyncio.sleep(3)
                continue

    def run_forever(self):
        try:
            asyncio.run(self.connect_socket_forever())
        except Exception as e:
            self.queue.put({"error": f"WebSocketClient error: {e}"})

    async def connect_socket_forever(self):
        while True:
            try:
                await self.connect_socket()
            except Exception as e:
                self.queue.put({"error": f"Reconnecting due to error: {e}"})
                await asyncio.sleep(3)


class WebSocketManager:
    def __init__(self, type: str, codes: list):
        self.type = type
        self.codes = codes
        self.url = "wss://api.upbit.com/websocket/v1"
        self.ws = None

    async def connect(self):
        try:
            self.ws = await websockets.connect(
                self.url,
                ping_interval=30,
                ping_timeout=60,
                max_queue=4096,
                close_timeout=3,
                max_size=None
            )
            subscribe_data = [
                {"ticket": str(uuid.uuid4())[:6]},
                {
                    "type": self.type,
                    "codes": self.codes,
                    "isOnlyRealtime": True
                }
            ]
            await self.ws.send(json.dumps(subscribe_data))
        except Exception as e:
            print(f"[WebSocketManager] Connect error: {e}")

    async def receive(self):
        if self.ws is None:
            raise RuntimeError("WebSocket is not connected. Call connect() first.")
        try:
            message = await asyncio.wait_for(self.ws.recv(), timeout=30)
            return json.loads(message)
        except asyncio.TimeoutError:
            print("[WebSocketManager] Timeout waiting for message. Reconnecting...")
            await self.connect()
            return None
        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
            print(f"[WebSocketManager] Disconnected: {e}. Reconnecting...")
            await self.connect()
            return None
        except Exception as e:
            print(f"[WebSocketManager] Error receiving message: {e}")
            await asyncio.sleep(1)
            return None

    async def close(self):
        if self.ws is not None:
            await self.ws.close()
            self.ws = None
