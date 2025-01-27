"""
Unit tests for the ColumnNameTransformer class.
"""

import pytest
import pandas as pd

from etl.transformers.dataframe_transformers import ColumnNameTransformer

def test_column_name_transformer():
    """
    Test that the ColumnNameTransformer class works
    """
    # create a test dataframe
    df = pd.DataFrame({"col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8]})

    # create the transformer
    transformer = ColumnNameTransformer({"col1": "new_col1", "col2": "new_col2"})

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (4, 2)
    assert data.columns.tolist() == ["new_col1", "new_col2"]
    assert data.values.tolist() == [[1, 5], [2, 6], [3, 7], [4, 8]]
    assert transformer.transformed_data_.equals(data)
    assert hasattr(transformer, "transformed_data_")