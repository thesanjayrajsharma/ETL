"""
Unit tests for the ColumnTypeValidator class.
"""

import pytest
import pandas as pd

from etl.validators.column_validators import ColumnTypeValidator

def test_column_type_validator():
    """
    Test that the ColumnTypeValidator class works
    """
    # create a test dataframe
    df = pd.DataFrame({"col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8]})

    # create the validator
    validator = ColumnTypeValidator({"col1": int, "col2": int})

    # validate the data
    validator.validate(df)

    # check that the validator is validated
    assert validator.validated_

    # re-run with an invalid column type
    with pytest.raises(ValueError):
        validator = ColumnTypeValidator({"col1": int, "col2": str})
        validator.validate(df)

    assert not validator.validated_