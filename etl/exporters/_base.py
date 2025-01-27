"""
Contains the base classes for all exporters.
These are meant to export data from the ETL pipeline to somewhere else.
The "L" in the ETL pipeline.  Export locations include databases and files.
"""
import os

from abc import ABCMeta, abstractmethod


class _BaseExporter(metaclass=ABCMeta):
    """Base class for all loaders"""

    @abstractmethod
    def export(self, data):
        """Export the data"""
        pass


class FileExporter(_BaseExporter):
    """Base class for all file exporters

    Example Usage
    -------------
        >>> from src.Exporters import FileExporter
        >>> data = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        >>> exporter = FileExporter(file_name="test.csv", base_path = 'data/results')
        >>> exporter.export(data)
    """

    def __init__(self, file_name: str, base_path: str):
        """Initialize the class

        Args:
            file_name (str): Name of the file to export to
            base_path (str): Base path to export to
        """
        self.file_name = file_name
        self.base_path = base_path

    def export(self, data) -> None:
        """Export the data to defined file path

        Args:
            data (pd.DataFrame): Dataframe to export
        """
        self._export_to_file(data)

    def _export_to_file(self, data) -> None:
        """Private method to export the data to a file

        Args:
            data (pd.DataFrame): Dataframe to export
        """
        # Create the path if it doesn't exist
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

        # Export the data
        file_path = os.path.join(self.base_path, self.file_name)
        data.to_csv(file_path, index=False)


class _DatabaseExporter(_BaseExporter):
    """Base class for all database exporters"""

    pass
