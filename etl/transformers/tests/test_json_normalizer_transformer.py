"""
Unit tests for the JSON Normalizer Transformer class.
"""

import pytest
import pandas as pd

from etl.transformers.column_transformers import JSONColumnNormalizer

def test_json_column_normalizer():
    """
    Test that the JSONNormalizerTransformer class works
    """
    # create a test dataframe
    df = pd.DataFrame({"col1": ['{"key1": "value1", "key2": "value2"}']})

    # create the transformer
    transformer = JSONColumnNormalizer(col = 'col1', 
                                        new_col_names = None,
                                        drop_original_col = True,
                                        col_mapping = {
                                                "key1": "col1_key1",
                                                "key2": "col1_key2"
                                            },
                                        explode_col = False)

    # transform the data
    data = transformer.transform(df)

    # check that the data is correct
    assert isinstance(data, pd.DataFrame)
    assert data.shape == (1, 2)
    assert data.columns.tolist() == ["col1_key1", "col1_key2"]
    assert data.values.tolist() == [["value1", "value2"]]
    assert transformer.transformed_data_.equals(data)
    assert hasattr(transformer, "transformed_data_")

def test_json_column_normalizer_explode():
    """
    Test JSONColumnNormalizer works with explode method
    """

    data = pd.DataFrame({'norms': [[{'percentile': 41, 'reference': '2020', 'type': 'achievement'}],
                                    [{'percentile': 79, 'reference': '2020', 'type': 'achievement'}]]})

    transformer = JSONColumnNormalizer(col = 'norms',
                                        new_col_names = ['NORMS_PERCENTILE', 
                                                         'NORMS_REFERENCE', 
                                                         'NORMS_TYPE'],
                                        drop_original_col = True,
                                        explode_col = True)
    
    data = transformer.transform(data)

    assert isinstance(data, pd.DataFrame)
    assert data.shape == (2, 3)
    assert data.columns.tolist() == ['NORMS_PERCENTILE', 'NORMS_REFERENCE', 'NORMS_TYPE']
    assert data.values.tolist() == [[41, '2020', 'achievement'], [79, '2020', 'achievement']]
    assert transformer.transformed_data_.equals(data)
    assert hasattr(transformer, "transformed_data_")
