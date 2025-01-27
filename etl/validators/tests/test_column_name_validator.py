"""
Unit tests for the ColumnNameValidator class.
"""

import pytest
import pandas as pd

from etl.validators.column_validators import ColumnNameValidator

def test_column_name_validator():
    """
    Test that the ColumnNameValidator class works
    """
    # create a test dataframe
    df = pd.DataFrame({"col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8]})

    # create the validator
    validator = ColumnNameValidator(["col1", "col2"])

    # validate the data
    validator.validate(df)

    # check that the validator is validated
    assert validator.validated_

    # re-run with an invalid column name
    with pytest.raises(ValueError):
        validator = ColumnNameValidator(["col1", "col3"])
        validator.validate(df)

    assert not validator.validated_
