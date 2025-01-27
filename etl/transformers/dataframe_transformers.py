"""
Transformers that transform dataframes, as opposed to single columns
"""

import pandas as pd

from etl.transformers._base import _BaseDataFrameTransformer

class ColumnNameTransformer(_BaseDataFrameTransformer):
    """
    Transformer to rename columns in a dataframe

    Example Usage
    -------------
        >>> from transformers.dataframe_transformers import ColumnNameTransformer
        >>> df = pd.DataFrame({'col1': [1, 2, 3, 4],
                               'col2': [5, 6, 7, 8]})
        >>> transformer = ColumnNameTransformer({'col1': 'new_col1', 'col2': 'new_col2'})
        >>> transformer.transform(df)
        >>> new_col1 new_col2
        >>> 0        5
    """

    def __init__(self, col_mapping: dict):
        """Initialize the class
        
        Args:
            col_mapping (dict): Mapping of old column names to new column names
        """
        super().__init__()
        self.col_mapping = col_mapping

    def transform(self, data) -> pd.DataFrame:
        """Rename dataframe columns according to the mapping
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self._transform_dataframe(data)

        return self.transformed_data_
    
    def _transform_dataframe(self, data) -> None:
        """Private method to transform the dataframe
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self.transformed_data_ = data.rename(columns=self.col_mapping)

class ColumnTypeTransformer(_BaseDataFrameTransformer):
    """
    Transformer to convert columns to a specific type
    
    Example Usage
    -------------
        >>> from transformers.dataframe_transformers import ColumnTypeTransformer
        >>> df = pd.DataFrame({'col1': ['1', '2', '3', '4'],
                               'col2': [5.2, 6.7, 7.3, 8.1]})
        >>> transformer = ColumnTypeTransformer({'col1': float, 'col2': int})
        >>> data = transformer.transform(data)
        >>> new_col1 new_col2
        >>> 1.        5
    """
    
    def __init__(self, col_types: dict):
        """Initialize the class
        
        Args:
            col_types (dict): Mapping of column names to types
        """
        super().__init__()
        self.col_types = col_types

    def transform(self, data) -> pd.DataFrame:
        """Transform the dataframe's data types
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self._transform_dataframe(data)

        return self.transformed_data_
    
    def _transform_dataframe(self, data) -> None:
        """Private method to transform the dataframe
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self.transformed_data_ = data.astype(self.col_types)

class ColumnDropTransformer(_BaseDataFrameTransformer):
    """
    Transformer to drop columns from a dataframe
    
    Example Usage
    -------------
        >>> from transformers.dataframe_transformers import ColumnDropTransformer
        >>> df = pd.DataFrame({'col1': [1, 2, 3, 4],
                               'col2': [5, 6, 7, 8]})
        >>> transformer = ColumnDropTransformer(['col1'])
        >>> data = transformer.transform(data)
        >>> col2
        >>> 5
    """
    
    def __init__(self, cols_to_drop: list):
        """Initialize the class
        
        Args:
            cols_to_drop (list): List of columns to drop
        """
        super().__init__()
        self.cols_to_drop = cols_to_drop

    def transform(self, data) -> pd.DataFrame:
        """Drop columns from the dataframe
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self._transform_dataframe(data)

        return self.transformed_data_
    
    def _transform_dataframe(self, data) -> None:
        """Private method to transform the dataframe
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self.transformed_data_ = data.drop(columns=self.cols_to_drop)

class DuplicateValueTransformer(_BaseDataFrameTransformer):
    """
    Transformer to drop duplicate values from a dataframe

    Example Usage
    -------------
        >>> from transformers.dataframe_transformers import DuplicateValueTransformer
        >>> df = pd.DataFrame({'col1': [1, 1, 3, 4],
                               'col2': [5, 5, 7, 8]})
        >>> transformer = DuplicateValueTransformer()
        >>> data = transformer.transform(data)
        >>> data.shape
        >>> (3, 2)
        >>> data
        >>> col1 col2
        >>> 1    5
        >>> 3    7
        >>> 4    8
    """

    def __init__(self, 
                 keep: str = 'first',
                 subset: list = None):
        """Initialize the class

        Args:
            keep (str): Which duplicate values to keep, either 'first', 'last', or False
            subset (list): List of columns to check for duplicates
        """
        super().__init__()
        self.keep = keep
        self.subset = subset

    def transform(self, data) -> pd.DataFrame:
        """Drop duplicate values from the dataframe

        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self._transform_dataframe(data)

        return self.transformed_data_
    
    def _transform_dataframe(self, data) -> None:
        """Private method to transform the dataframe

        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        self.transformed_data_ = data.drop_duplicates(keep=self.keep, subset=self.subset)

