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
    assert rec is not None  # caller must validate before calling this function
    label = f"Event {rec['event_id']}" if rec["event_id"] < event_id else f"Event {rec['event_id']} (latest)"
    print(f"\n--- Bond '{bond_id}' — {label} ---")
    print_table(
        ["BondID", "Position", "Clean Price", "Dirty Price", "PV", "PV Change"],
        [[rec["bond_id"], rec["position"],
          f"{rec['clean_price']:.4f}", f"{rec['dirty_price']:.4f}",
          f"{rec['pv']:.2f}", f"{rec['pv_change']:.2f}"]],
    )


def report_desk_at(ledger: list, event_id: int) -> None:
    desk_pvs = query_desk_at(ledger, event_id)
    if not desk_pvs:
        print("  No events found.")
        return
    latest = max(r["event_id"] for r in ledger if r["event_id"] <= event_id)
    label = f"Event {latest}" if latest < event_id else f"Event {latest} (latest)"
    print(f"\n--- Total PV by Desk — {label} ---")
    print_table(
        ["Desk", "Total PV"],
        [[desk, f"{pv:.2f}"] for desk, pv in sorted(desk_pvs.items())],
    )


def report_trader_at(ledger: list, event_id: int) -> None:
    trader_pvs = query_trader_at(ledger, event_id)
    if not trader_pvs:
        print("  No events found.")
        return
    latest = max(r["event_id"] for r in ledger if r["event_id"] <= event_id)
    label = f"Event {latest}" if latest < event_id else f"Event {latest} (latest)"
    print(f"\n--- Total PV by Trader — {label} ---")
    print_table(
        ["Trader", "Total PV"],
        [[trader, f"{pv:.2f}"] for trader, pv in sorted(trader_pvs.items())],
    )


# ─────────────────────────────────────────────
# String parser
# ─────────────────────────────────────────────

HELP_TEXT = """
Commands:
  bond <BOND_ID>                 position / clean price / dirty price / PV for a bond (latest state)
  bond <BOND_ID> at <EVENT_ID>   position / clean price / dirty price / PV for a bond as of a specific event
  desk                           total PV per desk (latest state)
  desk at <EVENT_ID>             total PV per desk as of a specific event
  trader                         total PV per trader (latest state)
  trader at <EVENT_ID>           total PV per trader as of a specific event
  help                           show this message
  exit | quit                    quit
"""

def parse_and_execute(raw: str, ledger: list, bonds: dict) -> bool:
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
        # bond <BOND_ID>  OR  bond <BOND_ID> at <EVENT_ID>
        bond_id: str = ""
        event_id: int = ledger[-1]["event_id"]
        if len(parts) == 2:
            bond_id = parts[1].upper()
        elif len(parts) == 4 and parts[2].lower() == "at" and parts[3].isdigit():
            bond_id, event_id = parts[1].upper(), int(parts[3])
        else:
            print("  Please follow the command format: bond <BOND_ID>  or  bond <BOND_ID> at <EVENT_ID>")
            return True

        # Stage 1: is the bond ID valid?
        if bond_id not in bonds:
            valid = ", ".join(sorted(bonds.keys()))
            print(f"  Bond '{bond_id}' does not exist. Valid bonds: {valid}")
            return True

        # Stage 2: does this bond have any trades up to the requested event?
        if not any(r["bond_id"] == bond_id and r["event_id"] <= event_id for r in ledger):
            first_trade = next((r["event_id"] for r in ledger if r["bond_id"] == bond_id), None)
            if first_trade is None:
                print(f"  Bond '{bond_id}' has no trades in the dataset.")
            else:
                print(f"  Bond '{bond_id}' has no trades at or before Event {event_id}. First trade is at Event {first_trade}.")
            return True

        report_bond_at(ledger, bond_id, event_id)

    elif cmd in ("desk", "trader"):
        # desk  OR  desk at <EVENT_ID>  (same for trader)
        latest = ledger[-1]["event_id"]
        if len(parts) == 1:
            event_id = latest
        elif len(parts) == 3 and parts[1].lower() == "at" and parts[2].isdigit():
            event_id = int(parts[2])
        else:
            print(f"  Please follow the command format: {cmd}  or  {cmd} at <EVENT_ID>")
            return True
        if cmd == "desk":
            report_desk_at(ledger, event_id)
        else:
            report_trader_at(ledger, event_id)

    else:
        print(f"  Unknown command '{cmd}'. Type 'help' to see available commands.")

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

        if not parse_and_execute(raw, ledger, bonds):
            break


if __name__ == "__main__":
    main()