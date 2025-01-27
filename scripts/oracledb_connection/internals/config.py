import pandas as pd


def apply_eval(series_val):
    """Helper function to apply eval to a series value"""
    if type(series_val) in [str, bytes]:
        return eval(series_val)
    elif type(series_val) in [list, dict]:
        return series_val
    return None


def normalize_json_col(
    data: pd.DataFrame,
    json_col: str,
    new_col_names: list = None,
    col_mapping: dict = None,
    explode_col=False,
) -> pd.DataFrame:
    """Normalize a json column in a dataframe"""

    # touching the original data could disrupt later operations
    data_copy = data.copy(deep=True)

    # unnormalized columns are sometimes strings, so eval them
    # this step is slow, best to try and replace
    data_copy[json_col] = data_copy[json_col].apply(apply_eval)
    print("dsdsdsdssdsf",data_copy[json_col])
    print(data_copy.columns)

    if explode_col:
        # must ignore index to match against original dataset
        exploded_data = data_copy[json_col].explode(ignore_index=False)
        print(exploded_data)
        exploded_index = exploded_data.index
        normalized_data = pd.json_normalize(exploded_data)
        print("normalized colmyns",normalized_data.columns)
        normalized_data.columns = new_col_names
        normalized_data.index = exploded_index
        final_data = pd.merge(
            data_copy, normalized_data, left_index=True, right_index=True
        )
        print('final columns ',final_data.columns)
        # reset index to match original dataset
        final_data.reset_index(drop=True, inplace=True)
    else:
        data_copy["key"] = data_copy.index
        normalized_data = pd.json_normalize(data_copy[json_col])
        normalized_data["key"] = normalized_data.index
        final_data = normalized_data.merge(data_copy, on="key")
        final_data.drop("key", axis=1, inplace=True)

        # need to use map since final data has all columns
        final_data.rename(columns=col_mapping, inplace=True)

    final_data.drop(json_col, axis=1, inplace=True)

    return final_data
