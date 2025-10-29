import os
import contextlib
import asyncio
from typing import Any, Dict, List, Optional, Tuple

import asyncpg
from dotenv import load_dotenv


# Load env vars from .env if present
load_dotenv()


DB_DSN = os.getenv(
    "PG_DSN",
    # Fallback builds DSN from individual env vars for convenience
    None,
)

def _build_dsn() -> str:
    if DB_DSN:
        return DB_DSN
    user = os.getenv("PGUSER", "doadmin")
    pwd = os.getenv("PGPASSWORD", "")
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE", "defaultdb")
    sslmode = os.getenv("PGSSLMODE", "require")
    return f"postgres://{user}:{pwd}@{host}:{port}/{db}?sslmode={sslmode}"


SYMBOL = os.getenv("APP_SYMBOL", "xmrusdt")
TABLE = os.getenv("APP_TABLE", "timeframe_data")


class DB:
    def __init__(self) -> None:
        self.pool: Optional[asyncpg.Pool] = None

    async def init(self) -> None:
        dsn = _build_dsn()
        self.pool = await asyncpg.create_pool(dsn, min_size=1, max_size=5)

    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetch_last_bars(self, timeframe: str, limit: int = 500) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, open_price, high_price, low_price, close_price, total_volume
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, timeframe, SYMBOL, limit)
        # reverse to ascending time
        out: List[Dict[str, Any]] = []
        for r in reversed(rows):
            bar = {
                "time": int(r["bucket"]),
                "open": float(r["open_price"]),
                "high": float(r["high_price"]),
                "low": float(r["low_price"]),
                "close": float(r["close_price"]),
            }
            v = r.get("total_volume")
            if v is not None:
                try:
                    bar["volume"] = float(v)
                except Exception:
                    pass
            out.append(bar)
        return out

    async def fetch_tail_bars(self, timeframe: str, n: int = 5) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, open_price, high_price, low_price, close_price, total_volume
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, timeframe, SYMBOL, n)
        # keep order descending in return
        out: List[Dict[str, Any]] = []
        for r in rows:
            bar = {
                "time": int(r["bucket"]),
                "open": float(r["open_price"]),
                "high": float(r["high_price"]),
                "low": float(r["low_price"]),
                "close": float(r["close_price"]),
            }
            v = r.get("total_volume")
            if v is not None:
                try:
                    bar["volume"] = float(v)
                except Exception:
                    pass
            out.append(bar)
        return out

    async def fetch_footprint_cols(self, timeframe: str, limit: int = 300) -> List[Dict[str, Any]]:
        assert self.pool is not None
        sql = f"""
            SELECT bucket, price_levels, imbalances
            FROM {TABLE}
            WHERE timeframe = $1 AND symbol = $2
            ORDER BY bucket DESC
            LIMIT $3
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, timeframe, SYMBOL, limit)
        out: List[Dict[str, Any]] = []
        # parse price_levels JSON string in Python to avoid DB-specific JSON operators
        import json, math
        rows_list = list(reversed(rows))
        for r in rows_list:
            t = int(r["bucket"])
            raw = r["price_levels"] or "{}"
            raw_imb = r.get("imbalances") if isinstance(r, dict) else r["imbalances"]
            try:
                levels = json.loads(raw)
            except Exception:
                try:
                    levels = json.loads(raw.replace("'", '"'))
                except Exception:
                    levels = {}
            if not isinstance(levels, dict) or not levels:
                # even if no levels, try include empty imb
                imb_compact: List[List[int]] = []
                out.append({"t": t, "p0": None, "tick": None, "rows": [], "imb": imb_compact})
                continue
            prices = []
            for ps in levels.keys():
                try:
                    prices.append(float(ps))
                except Exception:
                    pass
            if not prices:
                out.append({"t": t, "p0": None, "tick": None, "rows": [], "imb": []})
                continue
            # infer tick
            s = sorted(set(round(p, 8) for p in prices))
            diffs = [round(s[i]-s[i-1], 8) for i in range(1,len(s)) if (s[i]-s[i-1])>0]
            tick = min(diffs) if diffs else 0.01
            for c in [0.0001,0.001,0.005,0.01,0.02,0.05,0.1,0.2,0.5,1.0]:
                if abs(tick-c) <= c*0.25:
                    tick = c; break
            pmin, pmax = min(prices), max(prices)
            p0 = math.floor(pmin/tick)*tick
            pN = math.ceil(pmax/tick)*tick
            n = int(round((pN-p0)/tick)) + 1
            bids = [0.0]*n; asks=[0.0]*n
            for ps, vals in levels.items():
                try:
                    price = float(ps)
                    bv = float(vals.get("buy_volume", 0.0) or 0.0)
                    sv = float(vals.get("sell_volume", 0.0) or 0.0)
                except Exception:
                    continue
                idx = int(round((price-p0)/tick))
                if 0 <= idx < n:
                    bids[idx]+=bv; asks[idx]+=sv
            rows_comp = []
            for i in range(n):
                b=bids[i]; a=asks[i]
                if b>0 or a>0:
                    rows_comp.append([i, round(b,6), round(a,6)])

            # ---- parse imbalances JSON and compress by row/side ----
            imb_compact: List[List[int]] = []
            best_by_side: Dict[Tuple[int,int], Tuple[int,int]] = {}
            # key: (row_index, side) → (kind, strength)
            try:
                imb_map = {}
                if raw_imb:
                    try:
                        imb_map = json.loads(raw_imb)
                    except Exception:
                        imb_map = json.loads(str(raw_imb).replace("'", '"'))
                if isinstance(imb_map, dict):
                    for ps, lst in imb_map.items():
                        try:
                            price = float(ps)
                        except Exception:
                            continue
                        i = int(round((price - p0)/tick)) if tick else 0
                        if i < 0 or i >= n:
                            continue
                        if not isinstance(lst, list):
                            continue
                        for entry in lst:
                            try:
                                itype = entry.get("imbalance_type")
                                strength_s = entry.get("imbalance_strength", "normal")
                            except Exception:
                                continue
                            # side: 1 = buy, 0 = sell
                            if itype in ("buy_same_level", "buy_diagonal"):
                                side = 1
                            elif itype in ("sell_same_level", "sell_diagonal"):
                                side = 0
                            else:
                                continue
                            kind = 0 if ("same_level" in itype) else 1  # 0 same, 1 diag
                            strength = 1 if strength_s == "normal" else 2 if strength_s == "strong" else 3
                            k = (i, side)
                            prev = best_by_side.get(k)
                            if prev is None:
                                best_by_side[k] = (kind, strength)
                            else:
                                prev_kind, prev_str = prev
                                # Prefer stronger; if equal, prefer diagonal (1) over same (0)
                                if strength > prev_str or (strength == prev_str and kind > prev_kind):
                                    best_by_side[k] = (kind, strength)
            except Exception:
                best_by_side = {}

            for (i, side), (kind, strength) in best_by_side.items():
                imb_compact.append([i, side, kind, strength])

            out.append({"t": t, "p0": round(p0,8), "tick": tick, "rows": rows_comp, "imb": imb_compact})
        return out


class BarsNotifier:
    """LISTEN/NOTIFY-based notifier with poller fallback.

    Usage: await BarsNotifier(db).start(timeframes=["1m","3m"]) and then read events via queue per tf.
    """

    def __init__(self, db: DB) -> None:
        self.db = db
        self.conn: Optional[asyncpg.Connection] = None
        self.listen_channels: List[str] = []
        self.queues: Dict[str, asyncio.Queue] = {}
        self._task: Optional[asyncio.Task] = None
        self._fallback_tasks: List[asyncio.Task] = []
        self._stop = asyncio.Event()

    async def start(self, timeframes: List[str]) -> None:
        # Create queues
        for tf in timeframes:
            self.queues.setdefault(tf, asyncio.Queue(maxsize=1000))
        # Try LISTEN
        try:
            dsn = _build_dsn()
            self.conn = await asyncpg.connect(dsn)
            for tf in timeframes:
                ch = f"bars_{tf}"
                self.listen_channels.append(ch)
                await self.conn.add_listener(ch, self._on_notify)
            self._task = asyncio.create_task(self._listen_loop())
        except Exception:
            # Fallback to pollers
            await self._start_pollers(timeframes)

    async def _listen_loop(self) -> None:
        # Keep the connection alive until stop
        assert self.conn is not None
        try:
            while not self._stop.is_set():
                await asyncio.sleep(0.25)
        finally:
            try:
                for ch in self.listen_channels:
                    await self.conn.remove_listener(ch, self._on_notify)
            except Exception:
                pass
            try:
                await self.conn.close()
            except Exception:
                pass

    def _on_notify(self, connection: asyncpg.Connection, pid: int, channel: str, payload: str) -> None:
        # Expected channel name: bars_<tf>
        try:
            tf = channel.split("_",1)[1]
        except Exception:
            return
        q = self.queues.get(tf)
        if not q:
            return
        try:
            q.put_nowait({"type":"notify","payload":payload})
        except asyncio.QueueFull:
            # Drop if overwhelmed; Broadcaster will reconcile
            pass

    async def _start_pollers(self, timeframes: List[str]) -> None:
        # Poll tail as a fallback
        for tf in timeframes:
            task = asyncio.create_task(self._poll_loop(tf))
            self._fallback_tasks.append(task)

    async def _poll_loop(self, tf: str) -> None:
        # Simple poller: push a tick every 200ms
        q = self.queues[tf]
        while not self._stop.is_set():
            try:
                await q.put({"type":"poll"})
            except Exception:
                pass
            await asyncio.sleep(0.2)

    def queue_for(self, tf: str) -> asyncio.Queue:
        return self.queues[tf]

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
        for t in self._fallback_tasks:
            t.cancel()
        self._fallback_tasks.clear()
