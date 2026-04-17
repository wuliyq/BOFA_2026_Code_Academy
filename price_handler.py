"""
price_handler.py
Calculates dirty price from clean price and accrued interest.
"""


def calc_dirty_price(clean_price: float, accrued_interest: float) -> float:
    """
    Dirty Price = Clean Price + Accrued Interest
    """
    return clean_price + accrued_interest
