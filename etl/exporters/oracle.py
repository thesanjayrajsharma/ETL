"""
    File for Oracle database loader, to connect with ADW, ATP, etc
"""

from etl.loaders._base import _DatabaseExporter


class OracleDatabaseExporter(_DatabaseExporter):
    """Class for loading data from Oracle databases"""

    def __init__(self, connection_string: str):
        """Initialize the class
        Args:
            connection_string (str): Connection string to use to connect to the database
        """
        super().__init__(connection_string)
