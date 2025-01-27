"""
Pipelines to run ETL steps
"""
from abc import ABCMeta, abstractmethod

import pandas as pd


class _BasePipeline(metaclass=ABCMeta):
    """Base class for all pipelines"""

    def run(self):
        """Run the pipeline"""
        return self._run()

    @abstractmethod
    def _run(self):
        """Run the pipeline"""
        pass


class ETLPipeline(_BasePipeline):
    """
    Pipeline to run ETL steps

    Example Usage
    -------------
        >>> from pipelines import ETLPipeline
        >>> pipeline = ETLPipeline(
        >>>     connector = NWEAAssessmentConnector(..args..),
        >>>     transformers = [JSONColumnNormalizer(..args..)],
        >>>     validators = [ColumnNameValidator(..args..)],
        >>>     loader = FileLoader(..args..))
        >>> pipeline.run()
    """

    def __init__(
        self, connector, exporter, transformers: list = [], validators: list = []
    ):
        """Initialize the class
        Args:
            connector (BaseConnector): Connector to use
            exporter (BaseLoader): Exporter to use to export data to somewhere else
            transformers (list): Transformers to use
            validators (list): Validators to use
        """
        self.connector = connector
        self.transformers = transformers
        self.validators = validators
        self.exporter = exporter
        self.validated_ = False

    def _run(self) -> pd.DataFrame:
        """Run the pipeline, in order of connector, transformers, validators

        Returns:
            pd.DataFrame: Final dataframe
        """
        data = self.connector.pull_data()
        self._run_transformers(data)
        self._run_validators()
        self.exporter.export(self.final_data_)

    def run(self) -> pd.DataFrame:
        """Public method to run the pipeline

        Returns:
            pd.DataFrame: Final dataframe
        """
        self._run()

    def _run_transformers(self, data) -> None:
        """Run the transformers, each transformer will transform the data
        that's passed into it

        Returns:
            None
        """
        for transformer in self.transformers:
            data = transformer.transform(data)

        self.final_data_ = data

    def _run_validators(self) -> None:
        """Run the validators, each validator will validate the data
        that's passed into it

        Returns:
            None
        """
        for validator in self.validators:
            validator.validate(self.final_data_)

        self.validated_ = True
