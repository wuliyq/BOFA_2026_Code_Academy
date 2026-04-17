"""
main.py  –  Bond Aggregation Engine
========================================
Files expected in the same directory:
  bond.csv      – bond reference data
  position.csv  – trade events (EventID order)

REPL commands:
  bond <BOND_ID> at <EVENT_ID>   position / dirty price / PV for a bond as of an event
  desk at <EVENT_ID>             total PV per desk as of an event
  trader at <EVENT_ID>           total PV per trader as of an event
  help                           show this list
  exit / quit                    quit
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
    events = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            events.append({
                "event_id": int(row["EventID"]),
                "desk": row["Desk"],
                "trader": row["Trader"],
                "bond_id": row["BondID"],
                "buy_sell": row["BuySell"],
                "quantity": int(row["Quantity"]),
                "clean_price": float(row["CleanPrice"]),
            })
    return sorted(events, key=lambda e: e["event_id"])


# ─────────────────────────────────────────────
# Core processing
# ─────────────────────────────────────────────

def process_all_events(bonds: dict, events: list):
    tracker = PositionTracker()
    pv_tracker = PVTracker()
    ledger = []
    latest_dirty = {}

    for event in events:
        bond_id = event["bond_id"]
        bond = bonds[bond_id]

        # 1. Accrued interest & dirty price
        ai = calc_accrued_interest(bond["coupon"], bond["months_since_coupon"], bond["frequency"])
        dirty = calc_dirty_price(event["clean_price"], ai)
        latest_dirty[bond_id] = dirty

        tracker.process_event(event)
        position = tracker.get_position(bond_id)

        pv = calc_pv(position, dirty)
        pv_change = pv_tracker.record(event["event_id"], bond_id, pv)

        ledger.append({
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
        })

    return tracker, pv_tracker, ledger, latest_dirty


# ─────────────────────────────────────────────
# Snapshot helpers
# (rebuild state from ledger up to a given event)
# ─────────────────────────────────────────────

def snapshot_at(ledger: list, event_id: int) -> tuple[list, dict]:
    """
    Returns (slice, dirty_at) where:
      slice     – ledger rows with event_id <= event_id
      dirty_at  – {bond_id: dirty_price} as of that event (latest price seen up to that point)
    """
    sliced = [r for r in ledger if r["event_id"] <= event_id]
    dirty_at = {}
    for r in sliced:
        dirty_at[r["bond_id"]] = r["dirty_price"]
    return sliced, dirty_at


def positions_at(sliced: list) -> dict:
    """Net {bond_id: position} from a ledger slice."""
    pos = {}
    for r in sliced:
        pos[r["bond_id"]] = r["position"]   # ledger already stores cumulative position
    return pos


def desk_positions_at(sliced: list) -> dict:
    """
    {desk: {bond_id: position}} as of the slice.
    Rebuilds from individual trade deltas since the ledger stores net position,
    not per-desk position.
    """
    desk_pos = {}
    for r in sliced:
        desk = r["desk"]
        bid  = r["bond_id"]
        sign = 1 if r["buy_sell"] == "BUY" else -1
        delta = sign * r["quantity"]
        desk_pos.setdefault(desk, {})
        desk_pos[desk][bid] = desk_pos[desk].get(bid, 0) + delta
    return desk_pos


def trader_positions_at(sliced: list) -> dict:
    """{trader: {bond_id: position}} as of the slice."""
    trader_pos = {}
    for r in sliced:
        trader = r["trader"]
        bid    = r["bond_id"]
        sign   = 1 if r["buy_sell"] == "BUY" else -1
        delta  = sign * r["quantity"]
        trader_pos.setdefault(trader, {})
        trader_pos[trader][bid] = trader_pos[trader].get(bid, 0) + delta
    return trader_pos


# ─────────────────────────────────────────────
# Query functions
# ─────────────────────────────────────────────

def query_bond_at(ledger: list, bond_id: str, event_id: int) -> dict | None:
    """Latest ledger row for bond_id at or before event_id."""
    matches = [r for r in ledger if r["bond_id"] == bond_id and r["event_id"] <= event_id]
    return matches[-1] if matches else None


def query_desk_at(ledger: list, event_id: int) -> dict:
    """{desk: total_pv} as of event_id."""
    sliced, dirty_at = snapshot_at(ledger, event_id)
    desk_pos = desk_positions_at(sliced)
    return {
        desk: sum(pos * dirty_at.get(bid, 0) for bid, pos in bonds.items())
        for desk, bonds in desk_pos.items()
    }


def query_trader_at(ledger: list, event_id: int) -> dict:
    """{trader: total_pv} as of event_id."""
    sliced, dirty_at = snapshot_at(ledger, event_id)
    trader_pos = trader_positions_at(sliced)
    return {
        trader: sum(pos * dirty_at.get(bid, 0) for bid, pos in bonds.items())
        for trader, bonds in trader_pos.items()
    }


# ─────────────────────────────────────────────
# Print helpers
# ─────────────────────────────────────────────

def print_table(headers: list, rows: list) -> None:
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
# Report printers
# ─────────────────────────────────────────────

def report_bond_at(ledger: list, bond_id: str, event_id: int) -> None:
    rec = query_bond_at(ledger, bond_id, event_id)
    if not rec:
        print(f"  [!] No data for bond '{bond_id}' at or before event {event_id}.")
        return
    print(f"\n--- Bond '{bond_id}' at Event {event_id} ---")
    print_table(
        ["BondID", "Position", "Dirty Price", "PV", "PV Change"],
        [[rec["bond_id"], rec["position"],
          f"{rec['dirty_price']:.4f}", f"{rec['pv']:.2f}", f"{rec['pv_change']:.2f}"]],
    )


def report_desk_at(ledger: list, event_id: int) -> None:
    desk_pvs = query_desk_at(ledger, event_id)
    if not desk_pvs:
        print(f"  [!] No desk data at or before event {event_id}.")
        return
    print(f"\n--- Total PV by Desk at Event {event_id} ---")
    print_table(
        ["Desk", "Total PV"],
        [[desk, f"{pv:.2f}"] for desk, pv in sorted(desk_pvs.items())],
    )


def report_trader_at(ledger: list, event_id: int) -> None:
    trader_pvs = query_trader_at(ledger, event_id)
    if not trader_pvs:
        print(f"  [!] No trader data at or before event {event_id}.")
        return
    print(f"\n--- Total PV by Trader at Event {event_id} ---")
    print_table(
        ["Trader", "Total PV"],
        [[trader, f"{pv:.2f}"] for trader, pv in sorted(trader_pvs.items())],
    )


# ─────────────────────────────────────────────
# String parser
# ─────────────────────────────────────────────

HELP_TEXT = """
Commands:
  bond <BOND_ID> at <EVENT_ID>   position / dirty price / PV for a bond as of an event
  desk at <EVENT_ID>             total PV per desk as of an event
  trader at <EVENT_ID>           total PV per trader as of an event
  help                           show this message
  exit | quit                    quit
"""

def parse_and_execute(raw: str, ledger: list) -> bool:
    """
    Parse a command string and execute the matching report.
    Returns False if the user wants to exit, True otherwise.

    Grammar:
      bond <BOND_ID> at <EVENT_ID>
      desk at <EVENT_ID>
      trader at <EVENT_ID>
    """
    parts = raw.strip().split()
    if not parts:
        return True

    cmd = parts[0].lower()

    if cmd in ("exit", "quit"):
        print("  Goodbye.")
        return False

    elif cmd == "help":
        print(HELP_TEXT)

    elif cmd == "bond":
        # Expected: bond <BOND_ID> at <EVENT_ID>
        if len(parts) != 4 or parts[2].lower() != "at" or not parts[3].isdigit():
            print("  Usage: bond <BOND_ID> at <EVENT_ID>")
        else:
            report_bond_at(ledger, parts[1].upper(), int(parts[3]))

    elif cmd in ("desk", "trader"):
        # Expected: desk at <EVENT_ID>  /  trader at <EVENT_ID>
        if len(parts) != 3 or parts[1].lower() != "at" or not parts[2].isdigit():
            print(f"  Usage: {cmd} at <EVENT_ID>")
        else:
            event_id = int(parts[2])
            if cmd == "desk":
                report_desk_at(ledger, event_id)
            else:
                report_trader_at(ledger, event_id)

    else:
        print(f"  [!] Unknown command '{cmd}'. Type 'help' for available commands.")

    return True


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
    print(f"  Bonds loaded    : {len(bonds)}")
    print(f"  Events processed: {len(events)}")
    print(f"{'='*65}")
    print("  Type 'help' for available commands.")
    print(f"{'='*65}\n")

    while True:
        try:
            raw = input("bond-engine> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Goodbye.")
            break

        if not parse_and_execute(raw, ledger):
            break


if __name__ == "__main__":
    main()