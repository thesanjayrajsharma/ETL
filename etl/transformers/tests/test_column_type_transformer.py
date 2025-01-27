"""
Unit tests to ensure that the ColumnTypeTransformer class works as expected.
"""
import pytest
import pandas as pd

from etl.transformers.dataframe_transformers import ColumnTypeTransformer

def test_column_type_transformer():
    """
    Test that the ColumnTypeTransformer class works as expected.
    """
    # create a test dataframe
    df = pd.DataFrame({'col1': ['1', '2', '3', '4'],
                       'col2': [5.2, 6.7, 7.3, 8.1]})

    # create a transformer
    transformer = ColumnTypeTransformer({'col1': float, 'col2': int})

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (4, 2)
    assert data.columns.tolist() == ['col1', 'col2']
    assert data['col1'].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert data['col2'].tolist() == [5, 6, 7, 8]