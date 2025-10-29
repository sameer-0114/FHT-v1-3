"""
Headless local order book for Binance USDS-M Futures XMRUSDT.

Implements the exact algorithm from:
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly

Behavior:
- Opens WebSocket FIRST and buffers depth updates.
- Fetches REST depth snapshot and aligns using lastUpdateId.
- Applies events where U <= lastUpdateId <= u, then enforces pu chaining.
- Resyncs from snapshot on any gap (pu mismatch).
- Prints top 5 bids/asks on every applied diff event.

Dependencies: aiohttp (async HTTP + WebSocket)
"""

import asyncio
from collections import deque
from decimal import Decimal, getcontext
import signal
from typing import Dict, List, Tuple
import platform

import aiohttp


# Increase precision to avoid float round issues on prices/qty
getcontext().prec = 28


SYMBOL = "XMRUSDT"  # USDS-M Futures symbol

FAPI_BASE = "https://fapi.binance.com"
SNAPSHOT_URL = f"{FAPI_BASE}/fapi/v1/depth?symbol={SYMBOL}&limit=1000"

# Using combined stream endpoint to match docs exactly
FSTREAM_BASE = "wss://fstream.binance.com"
WS_URL = f"{FSTREAM_BASE}/stream?streams={SYMBOL.lower()}@depth"  # or add @depth@100ms if desired


class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol.upper()
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.last_update_id: int | None = None
        self.prev_u: int | None = None

    def clear(self):
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None
        self.prev_u = None

    def load_snapshot(self, data: dict) -> None:
        self.last_update_id = int(data["lastUpdateId"])
        self.bids = {Decimal(p): Decimal(q) for p, q in data["bids"]}
        self.asks = {Decimal(p): Decimal(q) for p, q in data["asks"]}

    def apply_diff(self, b_updates: List[List[str]], a_updates: List[List[str]]) -> None:
        # Absolute quantities. qty == 0 removes the level.
        for p_str, q_str in b_updates:
            p = Decimal(p_str)
            q = Decimal(q_str)
            if q == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = q
        for p_str, q_str in a_updates:
            p = Decimal(p_str)
            q = Decimal(q_str)
            if q == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = q

    def top_n(self, n: int = 5) -> Tuple[List[Tuple[Decimal, Decimal]], List[Tuple[Decimal, Decimal]]]:
        top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:n]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:n]
        return top_bids, top_asks

    def print_top_5(self):
        bids, asks = self.top_n(5)
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        spread = (best_ask - best_bid) if (best_bid is not None and best_ask is not None) else None

        print("\n=== XMRUSDT L2 (Top 5) ===")
        print("Bids:")
        for p, q in bids:
            print(f"  {p} x {q}")
        print("Asks:")
        for p, q in asks:
            print(f"  {p} x {q}")
        if spread is not None:
            print(f"Best Bid: {best_bid} | Best Ask: {best_ask} | Spread: {spread}")
        else:
            print("Best Bid/Ask not available yet")


async def fetch_snapshot(session: aiohttp.ClientSession) -> dict:
    async with session.get(SNAPSHOT_URL, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def ws_reader(ws: aiohttp.ClientWebSocketResponse, queue: asyncio.Queue):
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = msg.json()
                event = data.get("data", data)  # support /ws (raw) or /stream (wrapped)
                # Only depth update events are relevant; Futures sends fields U,u,pu,b,a
                if "u" in event and "U" in event and "b" in event and "a" in event:
                    await queue.put(event)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    except asyncio.CancelledError:
        pass
    except Exception:
        # Let outer loop handle reconnection
        pass


async def run_order_book():
    book = LocalOrderBook(SYMBOL)

    while True:  # reconnect loop
        queue: asyncio.Queue = asyncio.Queue()
        try:
            timeout = aiohttp.ClientTimeout(total=None)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.ws_connect(
                    WS_URL,
                    heartbeat=20.0,
                    max_msg_size=0,
                ) as ws:
                    reader_task = asyncio.create_task(ws_reader(ws, queue))

                    # Step 2/3: Buffer events, then get snapshot
                    snap = await fetch_snapshot(session)
                    book.clear()
                    book.load_snapshot(snap)
                    last_update_id = book.last_update_id

                    # Step 4/5: Align with first event crossing lastUpdateId
                    synced = False
                    prev_u = None

                    while not synced:
                        ev = await queue.get()  # wait for next event
                        U = int(ev["U"])  # firstUpdateId in event
                        u = int(ev["u"])  # finalUpdateId in event
                        # Drop anything with u < lastUpdateId
                        if u < last_update_id:
                            continue
                        # First processed must satisfy U <= lastUpdateId <= u
                        if U <= last_update_id <= u:
                            # Apply and become synced
                            book.apply_diff(ev.get("b", []), ev.get("a", []))
                            prev_u = u
                            book.prev_u = prev_u
                            synced = True
                            book.print_top_5()
                        # else keep waiting for the crossing event

                    # Step 6+: Process the live stream with pu continuity
                    while True:
                        ev = await queue.get()
                        U = int(ev["U"])  # not strictly needed post-sync
                        u = int(ev["u"])  # current finalUpdateId
                        pu = int(ev.get("pu", -1))

                        # Verify continuity
                        if pu != prev_u:
                            # Resync from step 3
                            snap = await fetch_snapshot(session)
                            book.clear()
                            book.load_snapshot(snap)
                            last_update_id = book.last_update_id
                            prev_u = None
                            synced = False
                            # Re-enter alignment sub-loop
                            while not synced:
                                ev = await queue.get()
                                U = int(ev["U"])  # noqa: F841
                                u = int(ev["u"])  # noqa: F841
                                if u < last_update_id:
                                    continue
                                if U <= last_update_id <= u:
                                    book.apply_diff(ev.get("b", []), ev.get("a", []))
                                    prev_u = u
                                    book.prev_u = prev_u
                                    synced = True
                                    book.print_top_5()
                            # Continue to next event after resync
                            continue

                        # Apply in-order event
                        book.apply_diff(ev.get("b", []), ev.get("a", []))
                        prev_u = u
                        book.prev_u = prev_u
                        book.print_top_5()

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"[warn] Stream error: {e}. Reconnecting in 1s...")
            await asyncio.sleep(1)
        finally:
            # Nothing explicit; context managers will close ws/session and end reader
            pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop):
    # Graceful Ctrl+C on Windows/POSIX
    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
        loop.add_signal_handler(signal.SIGTERM, loop.stop)
    except NotImplementedError:
        # Signals not available on some platforms (e.g., Windows Python<3.8)
        pass


def main():
    print("Starting Binance USDS-M Futures local order book (XMRUSDT)...")
    print("Press Ctrl+C to exit.")
    # On Windows, aiodns/aiohttp require Selector loop
    if platform.system() == "Windows":
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
    try:
        asyncio.run(run_order_book())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
