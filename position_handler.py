"""
position_handler.py
Tracks net positions per bond, desk, and trader as events are processed.
"""

from collections import defaultdict


class PositionTracker:
    def __init__(self):
        # Net position across all desks/traders
        self._bond_pos = defaultdict(int)
        # Position broken down by desk  {desk -> {bond_id -> position}}
        self._desk_pos = defaultdict(lambda: defaultdict(int))
        # Position broken down by trader {trader -> {bond_id -> position}}
        self._trader_pos = defaultdict(lambda: defaultdict(int))

    def process_event(self, event: dict) -> int:
        """
        Apply a single event (BUY or SELL) and return the position delta.

        event keys expected:
            bond_id, buy_sell ('BUY'|'SELL'), quantity, desk, trader
        """
        bond_id = event["bond_id"]
        sign = 1 if event["buy_sell"] == "BUY" else -1
        delta = sign * event["quantity"]

        self._bond_pos[bond_id] += delta
        self._desk_pos[event["desk"]][bond_id] += delta
        self._trader_pos[event["trader"]][bond_id] += delta

        return delta

    def get_position(self, bond_id: str) -> int:
        """Net position for a single bond across all desks/traders."""
        return self._bond_pos[bond_id]

    def get_all_bond_positions(self) -> dict:
        """Returns {bond_id: net_position} for every bond traded so far."""
        return dict(self._bond_pos)

    def get_desk_positions(self) -> dict:
        """Returns {desk: {bond_id: position}}."""
        return {desk: dict(pos) for desk, pos in self._desk_pos.items()}

    def get_trader_positions(self) -> dict:
        """Returns {trader: {bond_id: position}}."""
        return {trader: dict(pos) for trader, pos in self._trader_pos.items()}
