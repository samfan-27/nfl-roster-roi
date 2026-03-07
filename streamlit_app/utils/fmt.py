def dollars_to_str(millions):
    try:
        val = float(millions)
    except Exception:
        return ""
    return f'${val:,.1f}M'
    