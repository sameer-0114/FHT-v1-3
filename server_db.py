import asyncio
import json
import os
import sys
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from db_async import DB, BarsNotifier
from XMR_L2 import LocalOrderBook, SYMBOL as L2_SYMBOL, FSTREAM_BASE, WS_URL, fetch_snapshot, ws_reader  # type: ignore
import aiohttp


APP_TITLE = "Realtime Liquidity (DB)"
SYMBOL = os.getenv("APP_SYMBOL", "xmrusdt").upper()

app = FastAPI(title=APP_TITLE)
app.add_middleware(GZipMiddleware, minimum_size=1024)

# Windows SelectorEventLoop for aiodns/aiohttp compatibility
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

# Serve static if present
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")
if os.path.isdir(WEB_DIR):
    app.mount("/web", StaticFiles(directory=WEB_DIR, html=True), name="web")
    @app.get("/")
    async def index_root() -> FileResponse:
        idx = os.path.join(WEB_DIR, "index.html")
        if os.path.isfile(idx):
            return FileResponse(idx)
        # fallback: serve any file listing disabled; return 404 if missing
        return FileResponse(idx)  # will 404 if not present


db = DB()
notifier: Optional[BarsNotifier] = None
reconnect_task: Optional[asyncio.Task] = None


@app.on_event("startup")
async def on_start() -> None:
    """Initialize DB and notifier without failing app startup if DB is down.

    On failure, schedule a background reconnect loop and keep the app serving / and /health.
    """
    global notifier, reconnect_task
    try:
        await db.init()
        notifier = BarsNotifier(db)
        await notifier.start(["1m", "3m", "5m", "15m"])
        print("DB connected and notifier started")
    except Exception as e:
        notifier = None
        print(f"DB init failed at startup: {e}")

        async def _reconnect_loop():
            backoff = 3.0
            while True:
                try:
                    await db.init()
                    _n = BarsNotifier(db)
                    await _n.start(["1m", "3m", "5m", "15m"])
                    # promote
                    global notifier
                    notifier = _n
                    print("DB reconnected; notifier started")
                    return
                except Exception as e:
                    print(f"DB reconnect failed: {e}")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 1.5, 30.0)

        reconnect_task = asyncio.create_task(_reconnect_loop())


@app.on_event("shutdown")
async def on_stop() -> None:
    global notifier, reconnect_task
    if notifier:
        await notifier.stop()
        notifier = None
    if reconnect_task:
        try:
            reconnect_task.cancel()
            await reconnect_task
        except Exception:
            pass
        reconnect_task = None
    await db.close()


@app.get("/api/bars")
async def api_bars(tf: str = "1m", limit: int = 1500):
    limit = max(10, min(5000, limit))
    if db.pool is None:
        return JSONResponse({"error": "db not connected"}, status_code=503)
    bars = await db.fetch_last_bars(tf, limit)
    return JSONResponse(bars)


@app.get("/api/footprint")
async def api_footprint(tf: str = "1m", limit: int = 300):
    if db.pool is None:
        return JSONResponse({"error": "db not connected"}, status_code=503)
    cols = await db.fetch_footprint_cols(tf, limit)
    return JSONResponse(cols)


class BarsBroadcaster:
    def __init__(self, tf: str) -> None:
        self.tf = tf
        self.clients: List[WebSocket] = []
        self.task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._last_sent_time: Optional[int] = None
        self._last_sent_close: Optional[float] = None
        self._last_sent_vol: Optional[float] = None

    async def start(self) -> None:
        self.task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except Exception:
                pass
            self.task = None

    async def _run(self) -> None:
        q: Optional[asyncio.Queue] = None
        while not self._stop.is_set():
            try:
                # Wait for notifier/queue to be available
                global notifier
                if notifier is None:
                    await asyncio.sleep(0.5)
                    continue
                if q is None:
                    q = notifier.queue_for(self.tf)
                # Wait for a signal or timeout to do a periodic reconcile
                try:
                    await asyncio.wait_for(q.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
                # Coalesce fast successive signals
                await asyncio.sleep(0.08)
                while q is not None and not q.empty():
                    try:
                        q.get_nowait()
                    except Exception:
                        break
                # Skip until DB connected
                if db.pool is None:
                    await asyncio.sleep(0.5)
                    continue
                # Fetch latest 1-2 bars to compute delta
                tail = await db.fetch_tail_bars(self.tf, 2)
                if not tail:
                    continue
                latest = tail[0]
                t = int(latest["time"]) if isinstance(latest["time"], (int, float)) else latest["time"]
                c = float(latest["close"]) if latest.get("close") is not None else None
                v = float(latest.get("volume", 0) or 0)

                changed = False
                if self._last_sent_time != t:
                    changed = True
                elif (self._last_sent_close is None or c is None) or (self._last_sent_close != c) or (self._last_sent_vol != v):
                    changed = True

                if changed:
                    payload = {"type": "update", "bar": latest}
                    await self._broadcast(payload)
                    self._last_sent_time = t
                    self._last_sent_close = c
                    self._last_sent_vol = v
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.25)

    async def _broadcast(self, obj: Dict[str, Any]) -> None:
        if not self.clients:
            return
        msg = json.dumps(obj)
        stale: List[WebSocket] = []
        for ws in self.clients:
            try:
                await ws.send_text(msg)
            except Exception:
                stale.append(ws)
        for ws in stale:
            try:
                self.clients.remove(ws)
            except ValueError:
                pass


broads: Dict[str, BarsBroadcaster] = {}


def get_broadcaster(tf: str) -> BarsBroadcaster:
    b = broads.get(tf)
    if not b:
        b = BarsBroadcaster(tf)
        broads[tf] = b
        asyncio.create_task(b.start())
    return b


@app.websocket("/ws/bars")
async def ws_bars(ws: WebSocket):
    await ws.accept()
    tf = ws.query_params.get("tf", "1m")
    b = get_broadcaster(tf)
    b.clients.append(ws)
    # On connect, send a small seed (last bar) so client can sync
    try:
        # Send a small snapshot (last 300 bars) for context
        snap = await db.fetch_last_bars(tf, 300)
        if snap:
            await ws.send_text(json.dumps({"type":"snapshot", "series": snap}))
        # Also send the latest bar explicitly
        tail = await db.fetch_tail_bars(tf, 1)
        if tail:
            await ws.send_text(json.dumps({"type":"update", "bar": tail[0]}))
        while True:
            # Keep alive
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        pass
    except Exception:
        await asyncio.sleep(0.1)
    finally:
        try:
            b.clients.remove(ws)
        except ValueError:
            pass


class DOMBroadcaster:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self.last_payload: Optional[dict] = None

    async def add_client(self, ws: WebSocket):
        async with self._lock:
            self.clients.add(ws)
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def remove_client(self, ws: WebSocket):
        async with self._lock:
            self.clients.discard(ws)

    async def _broadcast(self, payload: dict):
        if not self.clients:
            self.last_payload = payload
            return
        self.last_payload = payload
        data = json.dumps(payload)
        dead: List[WebSocket] = []
        for ws in list(self.clients):
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.remove_client(ws)

    async def _run(self):
        book = LocalOrderBook(L2_SYMBOL)
        queue: asyncio.Queue = asyncio.Queue()

        def _compact_snapshot(
            book: LocalOrderBook, levels: Optional[int] = None
        ) -> tuple[list[tuple[str, str]], list[tuple[str, str]], Optional[float]]:
            bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
            asks = sorted(book.asks.items(), key=lambda x: x[0])
            if levels is not None:
                bids = bids[:levels]
                asks = asks[:levels]
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            mid = None
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2.0
            cbids = [(format(p, 'f'), format(q, 'f')) for p, q in bids]
            casks = [(format(p, 'f'), format(q, 'f')) for p, q in asks]
            return cbids, casks, mid

        backoff = 1.0
        while True:
            try:
                timeout = aiohttp.ClientTimeout(total=None)
                connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
                async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                    async with session.ws_connect(
                        WS_URL,
                        heartbeat=20.0,
                        max_msg_size=0,
                    ) as ws:
                        reader_task = asyncio.create_task(ws_reader(ws, queue))

                        # Snapshot sync
                        snap = await fetch_snapshot(session)
                        book.clear()
                        book.load_snapshot(snap)
                        last_update_id = book.last_update_id

                        pre_bcast_cancel = False
                        async def pre_broadcaster():
                            while not pre_bcast_cancel:
                                await asyncio.sleep(0.5)
                                bids, asks, mid = _compact_snapshot(book)
                                await self._broadcast({"type": "dom", "mid": mid, "bids": bids, "asks": asks})

                        pre_bcast_task = asyncio.create_task(pre_broadcaster())

                        synced = False
                        prev_u = None
                        while not synced:
                            ev = await queue.get()
                            U = int(ev["U"])  # noqa
                            u = int(ev["u"])  # noqa
                            if u < last_update_id:
                                continue
                            if U <= last_update_id <= u:
                                book.apply_diff(ev.get("b", []), ev.get("a", []))
                                prev_u = u
                                book.prev_u = prev_u
                                synced = True

                        pre_bcast_cancel = True
                        try:
                            pre_bcast_task.cancel()
                        except Exception:
                            pass

                        last_payload = None
                        async def broadcaster():
                            while True:
                                await asyncio.sleep(0.25)
                                bids, asks, mid = _compact_snapshot(book)
                                payload = {"type": "dom", "mid": mid, "bids": bids, "asks": asks}
                                await self._broadcast(payload)

                        bcast_task = asyncio.create_task(broadcaster())

                        while True:
                            ev = await queue.get()
                            u = int(ev["u"])  # current
                            pu = int(ev.get("pu", -1))
                            if pu != book.prev_u:
                                bcast_task.cancel()
                                snap = await fetch_snapshot(session)
                                book.clear()
                                book.load_snapshot(snap)
                                last_update_id = book.last_update_id
                                synced = False
                                prev_u = None
                                while not synced:
                                    ev = await queue.get()
                                    U = int(ev["U"])  # noqa
                                    u = int(ev["u"])  # noqa
                                    if u < last_update_id:
                                        continue
                                    if U <= last_update_id <= u:
                                        book.apply_diff(ev.get("b", []), ev.get("a", []))
                                        prev_u = u
                                        book.prev_u = prev_u
                                        synced = True
                                bcast_task = asyncio.create_task(broadcaster())
                                continue

                            book.apply_diff(ev.get("b", []), ev.get("a", []))
                            book.prev_u = u

            except asyncio.CancelledError:
                break
            except Exception:
                if not self.clients:
                    return
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)


dom_broadcaster = DOMBroadcaster()


@app.websocket("/ws/dom")
async def ws_dom(ws: WebSocket):
    await ws.accept()
    await dom_broadcaster.add_client(ws)
    try:
        while True:
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        await dom_broadcaster.remove_client(ws)
        return


@app.get("/health")
async def health() -> JSONResponse:
    db_status = "up" if db.pool is not None else "down"
    return JSONResponse({"status": "ok", "db": db_status})


@app.on_event("startup")
async def startup_event():
    if not os.path.isdir(WEB_DIR):
        return
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            snap = await fetch_snapshot(session)
            book = LocalOrderBook(L2_SYMBOL)
            book.load_snapshot(snap)
            bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
            asks = sorted(book.asks.items(), key=lambda x: x[0])
            best_bid = float(bids[0][0]) if bids else None
            best_ask = float(asks[0][0]) if asks else None
            mid = (best_bid + best_ask) / 2.0 if best_bid is not None and best_ask is not None else None
            dom_broadcaster.last_payload = {
                "type": "dom",
                "mid": mid,
                "bids": [(format(p, 'f'), format(q, 'f')) for p,q in bids],
                "asks": [(format(p, 'f'), format(q, 'f')) for p,q in asks],
            }
    except Exception:
        pass


@app.get("/api/dom/last")
async def get_dom_last():
    if dom_broadcaster.last_payload is None:
        return JSONResponse({"error": "no dom yet"}, status_code=404)
    return JSONResponse(dom_broadcaster.last_payload)


@app.get("/api/dom/snapshot")
async def get_dom_snapshot(levels: int = -1):
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(resolver=aiohttp.ThreadedResolver())
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            snap = await fetch_snapshot(session)
            book = LocalOrderBook(L2_SYMBOL)
            book.load_snapshot(snap)
            def _compact(book: LocalOrderBook, levels_opt: Optional[int] = None):
                bids = sorted(book.bids.items(), key=lambda x: x[0], reverse=True)
                asks = sorted(book.asks.items(), key=lambda x: x[0])
                if levels_opt is not None:
                    bids = bids[:levels_opt]
                    asks = asks[:levels_opt]
                best_bid = float(bids[0][0]) if bids else None
                best_ask = float(asks[0][0]) if asks else None
                mid = None
                if best_bid is not None and best_ask is not None:
                    mid = (best_bid + best_ask) / 2.0
                cbids = [(format(p, 'f'), format(q, 'f')) for p, q in bids]
                casks = [(format(p, 'f'), format(q, 'f')) for p, q in asks]
                return {"type": "dom", "mid": mid, "bids": cbids, "asks": casks}
            levels_opt = None if levels is None or levels < 0 else int(levels)
            payload = _compact(book, levels_opt)
            return JSONResponse(payload)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


# Root index under / using static web dir
if os.path.isdir(WEB_DIR):
    # Root handler defined above in the static mount section; keep a single definition.
    pass


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server_db:app", host="127.0.0.1", port=8000, reload=False, log_level="info")
