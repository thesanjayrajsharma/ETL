"""
Unit tests for the column drop transformer.
"""

import pytest
import pandas as pd

from etl.transformers.dataframe_transformers import ColumnDropTransformer

def test_column_drop_transformer():
    """
    Test that the ColumnDropTransformer class works as expected.
    """
    # create a test dataframe
    df = pd.DataFrame({'col1': [1, 2, 3, 4],
                       'col2': [5, 6, 7, 8]})

    # create a transformer
    transformer = ColumnDropTransformer(['col1'])

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (4, 1)
    assert data.columns.tolist() == ['col2']
    assert data['col2'].tolist() == [5, 6, 7, 8]