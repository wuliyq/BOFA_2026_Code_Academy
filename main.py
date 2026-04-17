"""
main.py  –  Bond Aggregation Engine
========================================
Files expected in the same directory:
  bond.csv      – bond reference data
  position.csv  – trade events (EventID order)
"""

import csv
import io

from interest_handler import calc_accrued_interest
from price_handler import calc_dirty_price
from position_handler import PositionTracker
from pv_handler import PVTracker, calc_pv

BONDS_FILE = "bond.csv"
EVENTS_FILE = "position.csv"


# ─────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────

def load_bonds(filepath: str) -> dict:
    """
    Returns {bond_id: {coupon, frequency, months_since_coupon}}.
    Handles the blank first row that appears in bond.csv.
    """
    with open(filepath, newline="") as f:
        lines = [line for line in f if line.strip().replace(",", "")]
    reader = csv.DictReader(io.StringIO("".join(lines)))
    bonds = {}
    for row in reader:
        if row.get("BondID"):
            bonds[row["BondID"]] = {
                "coupon": float(row["Coupon"]),
                "frequency": int(row["Frequency"]),
                "months_since_coupon": float(row["MonthsSinceCoupon"]),
            }
    return bonds


def load_events(filepath: str) -> list:
    """Returns events sorted by EventID (ascending)."""
    events = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append(
                {
                    "event_id": int(row["EventID"]),
                    "desk": row["Desk"],
                    "trader": row["Trader"],
                    "bond_id": row["BondID"],
                    "buy_sell": row["BuySell"],
                    "quantity": int(row["Quantity"]),
                    "clean_price": float(row["CleanPrice"]),
                }
            )
    return sorted(events, key=lambda e: e["event_id"])


# ─────────────────────────────────────────────
# Core processing
# ─────────────────────────────────────────────

def process_all_events(bonds: dict, events: list):
    """
    Processes all events in order.
    Returns (tracker, pv_tracker, ledger, latest_dirty_price).
    """
    tracker = PositionTracker()
    pv_tracker = PVTracker()
    ledger = []
    latest_dirty = {}  # {bond_id: most recent dirty price}

    for event in events:
        bond_id = event["bond_id"]
        bond = bonds[bond_id]

        # 1. Accrued interest & dirty price
        ai = calc_accrued_interest(bond["coupon"], bond["months_since_coupon"], bond["frequency"])
        dirty = calc_dirty_price(event["clean_price"], ai)
        latest_dirty[bond_id] = dirty

        # 2. Update position
        tracker.process_event(event)
        position = tracker.get_position(bond_id)

        # 3. PV and PV change
        pv = calc_pv(position, dirty)
        pv_change = pv_tracker.record(event["event_id"], bond_id, pv)

        ledger.append(
            {
                "event_id": event["event_id"],
                "desk": event["desk"],
                "trader": event["trader"],
                "bond_id": bond_id,
                "buy_sell": event["buy_sell"],
                "quantity": event["quantity"],
                "clean_price": event["clean_price"],
                "accrued_interest": ai,
                "dirty_price": dirty,
                "position": position,
                "pv": pv,
                "pv_change": pv_change,
            }
        )

    return tracker, pv_tracker, ledger, latest_dirty


# ─────────────────────────────────────────────
# Query functions
# ─────────────────────────────────────────────

def query_bond(ledger: list, bond_id: str) -> dict | None:
    """Latest position / dirty price / PV for a specific bond."""
    bond_events = [r for r in ledger if r["bond_id"] == bond_id]
    return bond_events[-1] if bond_events else None


def query_by_desk(tracker: PositionTracker, latest_dirty: dict) -> dict:
    """Total PV per desk using each desk's own positions and latest dirty prices."""
    result = {}
    for desk, positions in tracker.get_desk_positions().items():
        result[desk] = sum(
            pos * latest_dirty[bid] for bid, pos in positions.items() if bid in latest_dirty
        )
    return result


def query_by_trader(tracker: PositionTracker, latest_dirty: dict) -> dict:
    """Total PV per trader."""
    result = {}
    for trader, positions in tracker.get_trader_positions().items():
        result[trader] = sum(
            pos * latest_dirty[bid] for bid, pos in positions.items() if bid in latest_dirty
        )
    return result


def query_pnl_since(pv_tracker: PVTracker, from_event_id: int) -> float:
    """Sum of PV changes for all events with EventID >= from_event_id."""
    return pv_tracker.get_pnl_since(from_event_id)


# ─────────────────────────────────────────────
# Print helpers
# ─────────────────────────────────────────────

def print_table(headers: list, rows: list) -> None:
    """Pretty-print a table with dynamic column widths."""
    col_widths = [
        max(len(str(headers[i])), max((len(str(r[i])) for r in rows), default=0))
        for i in range(len(headers))
    ]
    sep = "+" + "+".join("-" * (w + 2) for w in col_widths) + "+"
    fmt = "|" + "|".join(f" {{:<{w}}} " for w in col_widths) + "|"

    print(sep)
    print(fmt.format(*headers))
    print(sep)
    for row in rows:
        print(fmt.format(*[str(v) for v in row]))
    print(sep)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    bonds = load_bonds(BONDS_FILE)
    events = load_events(EVENTS_FILE)
    tracker, pv_tracker, ledger, latest_dirty = process_all_events(bonds, events)

    print(f"\n{'='*65}")
    print("  BOND AGGREGATION ENGINE")
    print(f"{'='*65}")
    print(f"  Bonds loaded : {len(bonds)}")
    print(f"  Events processed : {len(events)}")
    print(f"{'='*65}\n")

    # ── 1. Bond queries ──────────────────────────────────────────
    for bid in ["BOND1", "BOND2", "BOND3", "BOND4", "BOND5"]:
        rec = query_bond(ledger, bid)
        if rec:
            print(f"--- Bond Query: {bid} ---")
            print_table(
                ["BondID", "Position", "Dirty Price", "PV", "PV Change"],
                [
                    [
                        rec["bond_id"],
                        rec["position"],
                        f"{rec['dirty_price']:.4f}",
                        f"{rec['pv']:.2f}",
                        f"{rec['pv_change']:.2f}",
                    ]
                ],
            )
            print()

    # ── 2. Total PV by Desk ──────────────────────────────────────
    print("--- Total PV by Desk ---")
    desk_pvs = query_by_desk(tracker, latest_dirty)
    print_table(
        ["Desk", "Total PV"],
        [[desk, f"{pv:.2f}"] for desk, pv in sorted(desk_pvs.items())],
    )
    print()

    # ── 3. Total PV by Trader ────────────────────────────────────
    print("--- Total PV by Trader ---")
    trader_pvs = query_by_trader(tracker, latest_dirty)
    print_table(
        ["Trader", "Total PV"],
        [[trader, f"{pv:.2f}"] for trader, pv in sorted(trader_pvs.items())],
    )
    print()

    # ── 4. P&L since a given EventID ────────────────────────────
    for since in [1, 50, 100]:
        pnl = query_pnl_since(pv_tracker, since)
        print(f"P&L since Event {since:>3}: {pnl:>12.2f}")
    print()

    # ── 5. Full event ledger (last 15 events) ───────────────────
    print("--- Last 15 Events ---")
    headers = ["EventID", "Desk", "Trader", "BondID", "B/S", "Qty",
               "CleanPx", "AI", "DirtyPx", "Position", "PV", "dPV"]
    rows = [
        [
            r["event_id"], r["desk"], r["trader"], r["bond_id"],
            r["buy_sell"], r["quantity"],
            f"{r['clean_price']:.2f}", f"{r['accrued_interest']:.4f}",
            f"{r['dirty_price']:.4f}", r["position"],
            f"{r['pv']:.2f}", f"{r['pv_change']:.2f}",
        ]
        for r in ledger[-15:]
    ]
    print_table(headers, rows)


if __name__ == "__main__":
    main()
