import math


def dollars_to_str(millions):
    try:
        value = float(millions)
    except (TypeError, ValueError):
        return 'Unknown'
    return f'${value:,.1f}M' if math.isfinite(value) else 'Unknown'
