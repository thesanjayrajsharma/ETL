"""
Base classes for ETL orchestration

An orchestration is an object that loads in a list of entities to look up,
cutoff dates, and puts them through a pipeline with a connector, transformers,
validators, and an exporter.

Ie, if you run this, you'll run the entire job for a given list of entities to run an ETL
pipeline on.
"""

from abc import ABCMeta, abstractmethod

import pandas as pd


class _BaseOrchestrator(metaclass=ABCMeta):
    """Base class for all orchestrators"""

    def run(self):
        """Run the orchestrator"""
        return self._run()

    @abstractmethod
    def _run(self):
        """Run the orchestrator"""
        pass


class APIOrchestrator(_BaseOrchestrator):
    """Base class for all ETL orchestrators"""

    def __init__(self, entities: list, cutoff_dates: pd.DataFrame, pipeline):
        """Initialize the class
        Args:
            entities (list): List of entities to run the pipeline on
            cutoff_dates (dict): Mapping of entities to cutoff dates
            pipeline (ETLPipeline): Pipeline to run
        """
        self.entities = entities
        self.cutoff_dates = cutoff_dates
        self.pipeline = pipeline
