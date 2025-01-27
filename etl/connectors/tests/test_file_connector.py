"""
Test FileConnector class
"""

import pytest
import pandas as pd
from etl.connectors._base import FileConnector


class TestFileConnector:
    """
    Class for testing the FileLoader class
    """

    def test_load_pandas(self, tmp_path):
        """
        Test that the load_pandas method works

        Args:
            tmp_path (pathlib.Path): Temporary path for the test
        """
        # create a test file
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b,c\n1,2,3\n4,5,6\n")

        # load the data
        loader = FileConnector(method="pandas", file_path=test_file)
        loader.pull_data()

        # check that the data is correct
        assert isinstance(loader.pulled_data_, pd.DataFrame)
        assert loader.pulled_data_.shape == (2, 3)
        assert loader.pulled_data_.columns.tolist() == ["a", "b", "c"]
        assert loader.pulled_data_.values.tolist() == [[1, 2, 3], [4, 5, 6]]