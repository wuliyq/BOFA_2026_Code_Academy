"""
pv_handler.py
Calculates PV, tracks PV changes per event, and answers P&L queries.
"""

from collections import defaultdict


def calc_pv(position: int, dirty_price: float) -> float:
    """PV = position x dirty price  (face value 100 implicit in dirty price)."""
    return position * dirty_price


class PVTracker:
    def __init__(self):
        # Most recent PV per bond (used to compute the change on the next event)
        self._prev_pv = defaultdict(float)
        # Full history: list of dicts with event_id, bond_id, pv, pv_change
        self._history = []

    def record(self, event_id: int, bond_id: str, pv: float) -> float:
        """
        Record the PV after an event and return the change vs the previous PV
        for the same bond.
        """
        pv_change = pv - self._prev_pv[bond_id]
        self._prev_pv[bond_id] = pv
        self._history.append(
            {"event_id": event_id, "bond_id": bond_id, "pv": pv, "pv_change": pv_change}
        )
        return pv_change

    def get_pnl_since(self, from_event_id: int) -> float:
        """Sum of all PV changes for events with EventID >= from_event_id."""
        return sum(r["pv_change"] for r in self._history if r["event_id"] >= from_event_id)
