"""
Public Column Transformers for use in ETL interfaces
"""
import pandas as pd

from etl.transformers._base import _BaseSingleColumnTransformer
from etl.transformers._utils import apply_eval

# TO DO: add add logic to handle non-existent columns


class JSONColumnNormalizer(_BaseSingleColumnTransformer):
    """
    Transformer to convert JSON columns to normalized dataframes

    Example Usage
    -------------
        >>> from transformers.column_transformers import JSONColumnNormalizer
        >>> df = pd.DataFrame({'col1': [{"key1": "value1", "key2": "value2"}]})
        >>> transformer = JSONColumnNormalizer(df, 'col1')
        >>> transformer.transform()
        >>> transformer.df
          col1_key1 col1_key2
        0    value1    value2
    """

    def __init__(
        self,
        col: str,
        new_col_names: list = None,
        drop_original_col: bool = True,
        col_mapping: dict = None,
        explode_col: bool = False,
        missing_cols_mapping: dict = None,
    ):
        """Initialize the class

        Args:
            df (pd.DataFrame): Dataframe to transform
            col (str): Column to transform, should contain JSON data
            new_col_names (list): New column names to use
            drop_original_col (bool): Whether to drop the original column
            col_mapping (dict): Mapping of old column names to new column names
            explode_col (bool): Whether to explode the column
            missing_vals_mapping (dict): Mapping of missing values to replace
        """
        super().__init__()
        self.col = col
        self.new_col_names = new_col_names
        self.drop_original_col = drop_original_col
        self.col_mapping = col_mapping
        self.explode_col = explode_col
        self.missing_vals_mapping = missing_cols_mapping

    def transform(self, data) -> pd.DataFrame:
        """Transform the data

        Returns:
            pd.DataFrame: Transformed dataframe
        """
        self._transform_column(data)

        return self.transformed_data_

    def _transform_column(self, data) -> None:
        """Private method to transform the column

        Returns:
            None
        """
        self._normalize_json_column(data)

    def _normalize_json_column(self, data) -> None:
        """Normalize the JSON column

        Args:
            data (pd.DataFrame): Dataframe to transform

        Returns:
            pd.DataFrame: Dataframe with normalized JSON column
        """

        if self.col in data.columns:
            # touching the original data could disrupt later operations
            data_copy = data.copy(deep=True)

            # unnormalized columns are sometimes strings, so eval them
            # this step is slow, best to try and replace
            data_copy[self.col] = data_copy[self.col].apply(apply_eval)

            if self.explode_col:
                # must ignore index to match against original dataset
                exploded_data = data_copy[self.col].explode(ignore_index=False)
                exploded_index = exploded_data.index
                normalized_data = pd.json_normalize(exploded_data)
                normalized_data.columns = self.new_col_names
                normalized_data.index = exploded_index
                final_data = pd.merge(
                    data_copy, normalized_data, left_index=True, right_index=True
                )

                # reset index to match original dataset
                final_data.reset_index(drop=True, inplace=True)
            else:
                data_copy["key"] = data_copy.index
                normalized_data = pd.json_normalize(data_copy[self.col])
                normalized_data["key"] = normalized_data.index
                final_data = normalized_data.merge(data_copy, on="key")
                final_data.drop("key", axis=1, inplace=True)

                # need to use map since final data has all columns
                final_data.rename(columns=self.col_mapping, inplace=True)

            # this is a work around for api responses which don't have
            # all the columns -- due to problems with NWEA API
        else:
            for col, missing_vals in self.missing_vals_mapping.items():
                final_data[col] = missing_vals

        if self.drop_original_col:
            final_data.drop(self.col, axis=1, inplace=True)

        self.transformed_data_ = final_data
