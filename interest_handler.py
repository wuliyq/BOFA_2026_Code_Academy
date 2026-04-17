"""
interest_handler.py
Calculates accrued interest for a bond.
"""


def calc_accrued_interest(coupon: float, months_since_coupon: float, frequency: int) -> float:
    """
    Accrued Interest = Face Value (100) x Coupon x (MonthsSinceCoupon / 12)

    coupon              : annual coupon rate as a decimal (e.g. 0.05 = 5 %)
    months_since_coupon : months elapsed since the last coupon payment
    frequency           : number of coupon payments per year
    """
    return (coupon / frequency) * months_since_coupon / (12 / frequency) * 100
