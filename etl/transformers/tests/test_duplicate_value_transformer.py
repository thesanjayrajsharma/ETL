"""
Unit tests for the DuplicateValueTransformer class.
"""

import pytest
import pandas as pd

from etl.transformers.dataframe_transformers import DuplicateValueTransformer

def test_duplicate_value_transformer():
    """
    Test that the DuplicateValueTransformer class works as expected.
    """
    # create a test dataframe
    df = pd.DataFrame({'col1': [1, 1, 3, 4],
                       'col2': [5, 5, 7, 8]})

    # create a transformer
    transformer = DuplicateValueTransformer()

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (3, 2)
    assert data.columns.tolist() == ['col1', 'col2']
    assert data['col1'].tolist() == [1, 3, 4]
    assert data['col2'].tolist() == [5, 7, 8]

def test_duplicate_value_transformer_w_subset():
    """
    Test that the DuplicateValueTransformer class works as expected when
    a subset of columns is passed in.
    """
    # create a test dataframe
    df = pd.DataFrame({'col1': [1, 1, 1, 4],
                       'col2': [5, 3, 3, 8],
                       'col3': [9, 9, 11, 12]})

    # create a transformer
    transformer = DuplicateValueTransformer(subset=['col1', 'col2'])

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (3, 3)
    assert data.columns.tolist() == ['col1', 'col2', 'col3']
    assert data['col1'].tolist() == [1, 1, 4]
    assert data['col2'].tolist() == [5, 3, 8]
    assert data['col3'].tolist() == [9, 9, 12]
    assert hasattr(transformer, 'transformed_data_')

    # check for columns ['col1, 'col2'] duplicates

    # create a transformer
    transformer = DuplicateValueTransformer(subset=['col1', 'col2'])

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (3, 3)
    assert data.columns.tolist() == ['col1', 'col2', 'col3']
    assert data['col1'].tolist() == [1, 1, 4]
    assert data['col2'].tolist() == [5, 3, 8]
    assert data['col3'].tolist() == [9, 9, 12]