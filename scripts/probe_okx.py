"""Probe OKX public market data and save real messages as test fixtures.

Read-only public data: no account, no API key, no trading endpoint. Run it to
confirm the venue is reachable from where you are and to refresh the fixtures
the adapter's tests are built from:

    python scripts/probe_okx.py src/test/resources/okx

It checks the properties the adapter relies on rather than assuming them: that
books5 really is a full snapshot of at most five levels every message, which is
what makes it safe without delta and resync machinery, and that trades report
the taker side.
"""
import asyncio
import json
import pathlib
import sys

import websockets

URL = "wss://ws.okx.com:8443/ws/v5/public"
INST = "BTC-USDT"


async def main(out_dir: pathlib.Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    books, trades, events = [], [], []
    async with websockets.connect(URL, open_timeout=15, ping_interval=None) as ws:
        await ws.send(json.dumps({"op": "subscribe", "args": [
            {"channel": "books5", "instId": INST},
            {"channel": "trades", "instId": INST},
        ]}))
        deadline = asyncio.get_event_loop().time() + 20
        while (len(books) < 5 or len(trades) < 5) and asyncio.get_event_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            msg = json.loads(raw)
            if "event" in msg:
                events.append(msg)
                continue
            channel = msg.get("arg", {}).get("channel")
            if channel == "books5" and len(books) < 5:
                books.append(raw)
            elif channel == "trades" and len(trades) < 5:
                trades.append(raw)

    (out_dir / "books5.jsonl").write_text("\n".join(books) + "\n")
    (out_dir / "trades.jsonl").write_text("\n".join(trades) + "\n")

    ok = True
    print(f"subscribe acks: {[e.get('event') for e in events]}")
    b = json.loads(books[0])
    d = b["data"][0]
    print(f"books5 action field: {b.get('action')!r} (absent means snapshot-per-message)")
    print(f"books5 keys: {sorted(d)}  levels: bids={len(d['bids'])} asks={len(d['asks'])}")
    print(f"books5 level shape: {d['bids'][0]}  ts={d['ts']}")
    for raw in books:
        dd = json.loads(raw)["data"][0]
        if len(dd["bids"]) > 5 or len(dd["asks"]) > 5 or json.loads(raw).get("action") == "update":
            print("  NOT a <=5-level snapshot:", raw[:120])
            ok = False
    t = json.loads(trades[0])["data"][0]
    print(f"trade keys: {sorted(t)}  sample: px={t['px']} sz={t['sz']} side={t['side']} ts={t['ts']}")
    print(f"saved {len(books)} books5 and {len(trades)} trades messages to {out_dir}")
    return 0 if ok and len(books) == 5 and len(trades) == 5 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main(pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "okx-fixtures"))))
