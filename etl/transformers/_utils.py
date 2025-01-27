def apply_eval(series_val):
    """Helper function to apply eval to a series value"""
    if type(series_val) in [str, bytes]:
        return eval(series_val)
    elif type(series_val) in [list, dict]:
        return series_val
    return None