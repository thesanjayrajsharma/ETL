"""
Base classes for transformation steps in ETL pipelines
"""

from abc import ABCMeta, abstractmethod

class _BaseTransformer(metaclass=ABCMeta):
    """Base class for all transformers"""

    @abstractmethod
    def transform(self, data):
        """Transform the data
        
        Args:
            data (pd.DataFrame): Dataframe to transform
        """
        pass

class _BaseSingleColumnTransformer(_BaseTransformer):
    """Base class for single column transformers"""

    @abstractmethod
    def _transform_column(self):
        """Transform a column"""
        pass

class _BaseDataFrameTransformer(_BaseTransformer):
    """Base class for dataframe transformers"""

    @abstractmethod
    def _transform_dataframe(self):
        """Transform a dataframe"""
        pass
