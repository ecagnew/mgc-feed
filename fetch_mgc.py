"""
Pull MGC (micro gold) bars from Databento and write mgc.json for the trading agents.

- Source: Databento GLBX.MDP3 (CME Globex), 1-hour OHLCV, volume-rolled front month (MGC.v.0)
- Daily and 4H bars are rebuilt from the hourly bars on the CME session
  (6 PM ET -> 5 PM ET), so they line up with TradingView's MGC1! candles.
- Settlement prices come from the statistics schema when available.

Usage:
    DATABENTO_API_KEY=... python fetch_mgc.py --out mgc.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pandas as pd

DATASET = "GLBX.MDP3"
SYMBOL = "MGC.v.0"          # volume-based continuous front month
STYPE = "continuous"
ET = "America/New_York"
LOOKBACK_DAYS = 220         # enough hourly history for ~150 daily sessions
MAX_COST_USD = float(os.getenv("MAX_COST_USD", "0.25"))  # guardrail per run

KEEP_DAILY = 130
KEEP_H4 = 90
KEEP_H1 = 72


# ---------------------------------------------------------------- session math

def session_date(ts_et: pd.Timestamp):
    """CME gold session starts 6 PM ET; shifting +6h maps 18:00 to the next calendar day."""
    return (ts_et + timedelta(hours=6)).date()


def session_hour(ts_et: pd.Timestamp) -> int:
    """Hours since the 6 PM ET session open (0..22)."""
    return (ts_et.hour - 18) % 24


def to_et(hourly: pd.DataFrame) -> pd.DataFrame:
    """hourly: index = bar start (UTC, tz-aware), cols open/high/low/close/volume."""
    df = hourly[["open", "high", "low", "close", "volume"]].copy()
    df.index = df.index.tz_convert(ET)
    df["session"] = [session_date(t) for t in df.index]
    df["s_hour"] = [session_hour(t) for t in df.index]
    return df.sort_index()


def agg(group: pd.DataFrame) -> dict:
    return {
        "open": float(group["open"].iloc[0]),
        "high": float(group["high"].max()),
        "low": float(group["low"].min()),
        "close": float(group["close"].iloc[-1]),
        "volume": int(group["volume"].sum()),
    }


def session_closed(sess, now_et: pd.Timestamp) -> bool:
    close_time = pd.Timestamp(datetime.combine(sess, datetime.min.time())).tz_localize(ET) + timedelta(hours=17)
    return now_et >= close_time


def build_daily(df_et: pd.DataFrame, settles: dict, now_et: pd.Timestamp) -> list[dict]:
    rows = []
    for sess, g in df_et.groupby("session", sort=True):
        bar = {"date": sess.isoformat(), **agg(g)}
        bar["settle"] = settles.get(sess.isoformat())
        bar["complete"] = session_closed(sess, now_et)
        rows.append(bar)
    return rows


def build_h4(df_et: pd.DataFrame) -> list[dict]:
    """4H blocks anchored to the 6 PM ET open: 18, 22, 02, 06, 10, 14 ET."""
    rows = []
    blocks = df_et.assign(block=df_et["s_hour"] // 4)
    for (sess, blk), g in blocks.groupby(["session", "block"], sort=True):
        rows.append({"start_et": g.index[0].isoformat(), "session": sess.isoformat(), **agg(g)})
    return rows


def wilder_atr(daily: list[dict], n: int = 14) -> float | None:
    done = [d for d in daily if d["complete"]]
    if len(done) <= n:
        return None
    tr = []
    for prev, cur in zip(done, done[1:]):
        tr.append(max(cur["high"] - cur["low"], abs(cur["high"] - prev["close"]), abs(cur["low"] - prev["close"])))
    atr = sum(tr[:n]) / n
    for x in tr[n:]:
        atr = (atr * (n - 1) + x) / n
    return round(atr, 2)


def window_range(df_et: pd.DataFrame, sess, start_h: int, end_h: int) -> dict | None:
    """High/low of hourly bars starting in [start_h, end_h) ET clock hours, in session `sess`."""
    g = df_et[(df_et["session"] == sess) & (df_et.index.hour >= start_h) & (df_et.index.hour < end_h)]
    if g.empty:
        return None
    return {"high": float(g["high"].max()), "low": float(g["low"].min()), "bars": len(g)}


# ---------------------------------------------------------------- payload

def build_payload(hourly: pd.DataFrame, settles: dict, now_utc: datetime) -> dict:
    now_et = pd.Timestamp(now_utc).tz_convert(ET)
    df = to_et(hourly)
    daily = build_daily(df, settles, now_et)
    h4 = build_h4(df)
    done = [d for d in daily if d["complete"]]
    last_done = done[-1] if done else None
    prior = done[-2] if len(done) >= 2 else None
    current = daily[-1] if daily and not daily[-1]["complete"] else None
    last_bar = df.iloc[-1]

    h1 = [
        {"start_et": t.isoformat(), "open": float(r.open), "high": float(r.high),
         "low": float(r.low), "close": float(r.close), "volume": int(r.volume)}
        for t, r in df.tail(KEEP_H1).iterrows()
    ]

    latest_sess = df["session"].iloc[-1]
    return {
        "generated_at_utc": now_utc.replace(microsecond=0).isoformat(),
        "source": f"Databento {DATASET} ohlcv-1h, {SYMBOL} (volume-rolled front month)",
        "notes": "Daily/4H rebuilt from 1H on the CME session (6 PM-5 PM ET). 'close' = last trade; "
                 "'settle' = CME settlement when available. TradingView MGC1! daily bars use settlement as close by default.",
        "last_bar_start_et": df.index[-1].isoformat(),
        "last_price": float(last_bar.close),
        "last_complete_session": last_done,
        "prior_complete_session": prior,
        "current_session": current,
        "atr14_daily": wilder_atr(daily),
        "london_current_session": window_range(df, latest_sess, 3, 8),   # 3-8 AM ET
        "daily": daily[-KEEP_DAILY:],
        "h4": h4[-KEEP_H4:],
        "h1": h1,
    }


# ---------------------------------------------------------------- Databento I/O

def available_end(client, schema: str) -> pd.Timestamp:
    rng = client.metadata.get_dataset_range(dataset=DATASET)
    end = rng.get("schema", {}).get(schema, {}).get("end") or rng["end"]
    return pd.Timestamp(end)


def fetch_hourly(client, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    cost = client.metadata.get_cost(dataset=DATASET, symbols=[SYMBOL], stype_in=STYPE,
                                    schema="ohlcv-1h", start=start, end=end)
    print(f"ohlcv-1h estimated cost: ${cost:.4f}")
    if cost > MAX_COST_USD:
        sys.exit(f"Refusing: cost ${cost:.4f} > MAX_COST_USD ${MAX_COST_USD}")
    store = client.timeseries.get_range(dataset=DATASET, symbols=[SYMBOL], stype_in=STYPE,
                                        schema="ohlcv-1h", start=start, end=end)
    df = store.to_df()
    if df.empty:
        sys.exit("No hourly bars returned")
    return df


def fetch_settlements(client, start: pd.Timestamp, end: pd.Timestamp) -> dict:
    """{ 'YYYY-MM-DD': settle } keyed by trading date; empty dict on any failure."""
    try:
        cost = client.metadata.get_cost(dataset=DATASET, symbols=[SYMBOL], stype_in=STYPE,
                                        schema="statistics", start=start, end=end)
        print(f"statistics estimated cost: ${cost:.4f}")
        if cost > MAX_COST_USD:
            print("Skipping settlements: over cost guardrail")
            return {}
        df = client.timeseries.get_range(dataset=DATASET, symbols=[SYMBOL], stype_in=STYPE,
                                         schema="statistics", start=start, end=end).to_df()
        df = df[df["stat_type"] == 3]  # SETTLEMENT_PRICE
        out = {}
        for _, r in df.iterrows():
            ref = r.get("ts_ref")
            day = (pd.Timestamp(ref).date() if pd.notna(ref) and pd.Timestamp(ref).year > 2000
                   else session_date(pd.Timestamp(r.name).tz_convert(ET)))
            out[day.isoformat()] = float(r["price"])  # later updates overwrite preliminary
        return out
    except Exception as e:  # settlements are a nice-to-have
        print(f"Settlements unavailable: {e}")
        return {}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="mgc.json")
    args = ap.parse_args()

    key = os.getenv("DATABENTO_API_KEY")
    if not key:
        sys.exit("DATABENTO_API_KEY is not set")

    import databento as db
    client = db.Historical(key)

    end = available_end(client, "ohlcv-1h")
    start = end - timedelta(days=LOOKBACK_DAYS)
    hourly = fetch_hourly(client, start, end)
    settles = fetch_settlements(client, end - timedelta(days=30), available_end(client, "statistics"))

    payload = build_payload(hourly, settles, datetime.now(timezone.utc))
    with open(args.out, "w") as f:
        json.dump(payload, f, indent=1)
    # Logs are public on a public repo: report shape only, never prices.
    lc = payload["last_complete_session"] or {}
    print(f"Wrote {args.out}: {len(payload['daily'])} daily / {len(payload['h4'])} 4H / {len(payload['h1'])} 1H bars, "
          f"last bar {payload['last_bar_start_et']}, last session {lc.get('date')}, "
          f"settle present: {lc.get('settle') is not None}")


if __name__ == "__main__":
    main()
