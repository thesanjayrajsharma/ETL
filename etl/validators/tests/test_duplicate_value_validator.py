"""
Unit tests for the DuplicateValueValidator class.
"""

import pytest
import pandas as pd

from etl.validators.column_validators import DuplicateValueValidator


def test_duplicate_value_validator():
    """
    Test that the DuplicateValueValidator class works
    """
    # create a test dataframe
    df = pd.DataFrame({"col1": [1, 2, 3, 4], "col2": [5, 6, 7, 8]})

    # create the validator
    validator = DuplicateValueValidator(subset=["col1", "col2"])

    # validate the data
    validator.validate(df)

    # check that the validator is validated
    assert validator.validated_

    # re-run with duplicated data
    df = pd.DataFrame({"col1": [1, 2, 3, 4, 4], "col2": [5, 6, 7, 8, 8]})
    with pytest.raises(ValueError):
        validator = DuplicateValueValidator(subset=["col1", "col2"])
        validator.validate(df)

    assert len(validator.duplicates_) == 1
    assert validator.duplicates_[0] == [4, 8]
    assert not validator.validated_
