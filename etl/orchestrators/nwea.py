"""Orchestrator classes for the NWEA MAP Growth dataset.

NOTE:  This is a work in progress and is not yet complete.
This is where the work stopped for this project.  If time permits
I will continue working on this and complete it.
"""

import logging
import os

import pandas as pd

from etl.orchestrators._base import APIOrchestrator


class NWEAAssessmentOrchestrator(APIOrchestrator):
    """Orchestrator class for the NWEA MAP Growth dataset.
  
    Example Usage
    --------------
    pipeline = ETLPipeline(
        connector = NWEAAssessmentConnector(),
        transformers = [JSONColumnNormalizer(), ...,],
        validators = [DuplicateValueValidator()],
        exporters = FileExporter('../data/test.csv')
    )

    cutoffs = FileConnector('../data/nwea.csv')
    entities = Fileconnector('../data/students.csv')

    orchestrator = NWEAAssessmentOrchestrator(
        entities = entities,
        cutoffs = cutoffs,
        pipeline = pipeline
    )

    orchestrator.run()
    """

    def __init__(self, entities: list, cutoff_dates: dict, pipeline):
        """Initialize the class
        Args:
            entities (list): List of entities to run the pipeline on
            cutoff_dates (dict): Mapping of entities to cutoff dates
            pipeline (ETLPipeline): Pipeline to run
        """
        super().__init__(entities, cutoff_dates, pipeline)

    def _run(self):
        """Run the orchestrator"""
        # Loop through the entities
        for entity in self.entities:
            logging.info(f"Running pipeline for {entity}")

            if entity in self.cutoff_dates:
                logging.info(f"Found cutoff date for {entity}")
                cutoff_date = self.cutoff_dates[entity]
            else:
                cutoff_date = None

            # set the entity and cutoff date for the NWEA connector
            # WARNING:  maybe better to initialize a new connector for each entity
            self.pipeline.connector.bid = entity
            self.pipeline.connector.max_date = cutoff_date
            self.pipeline.connector.params = {"school-bid": entity}

            # Run the pipeline
            self.pipeline.run(entity, cutoff_date)

            # Export the data
            exporter = self.pipeline.exporter
            exporter.export(self.pipeline.data)
            logging.info(
                f"Exported data to {os.path.join(exporter.base_path, exporter.file_name)}"
            )

class NWEAStudentOrchestrator(APIOrchestrator):
    """
    Orchestrator for the NWEA Student dataset
    """
    def __init__(self, entities, cutoff_dates, pipeline):
        """
        Args:
        ------
        """
        self.entities = entities
        self.cutoff_dates = cutoff_dates
        self.pipeline = pipeline


    def _run(self):
        """
        Args:
        -----
        """

if __name__ == '__main__':
    # import the assessment connector,and file connector for loading cutoffs and entities
    from etl.connectors.nwea import NWEAAssessmentConnector
    from etl.connectors.file import FileConnector

    # initialize both to create a pipeline
    assessment_connector = NWEAAssessmentConnector()
    entity_connector = FileConnector('../data/students.csv')
    cutoff_connector = FileConnector('../data/nwea.csv')