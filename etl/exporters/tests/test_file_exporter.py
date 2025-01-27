"""
Unit tests for the FileExporter class.
"""

import pandas as pd

from etl.exporters._base import FileExporter


def test_file_exporter(tmp_path):
    """
    Test that the FileExporter class works
    """
    # create a test dataframe
    data = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # create a test file
    file_name = "test.csv"
    base_path = tmp_path
    file_path = tmp_path / file_name

    # export the data
    exporter = FileExporter(file_name=file_name, base_path=base_path)
    exporter.export(data)

    # check that the data is correct
    assert file_path.exists()
    assert file_path.read_text() == "a,b\n1,4\n2,5\n3,6\n"
