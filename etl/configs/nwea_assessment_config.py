"""
Contains all the configuration variables to run the NWEA Assessment ETL pipeline

TO DO: Convert to a yaml file that anyone can fill out
"""

import os
print(os.getcwd())
import numpy as np
import pandas as pd

from dotenv import load_dotenv

import sys
sys.path.append('..')

from etl.connectors.nwea import NWEAAssessmentConnector
from etl.connectors._base import FileConnector
from etl.transformers.column_transformers import JSONColumnNormalizer
from etl.exporters._base import FileExporter
from etl.pipelines._base import ETLPipeline
from etl.transformers.dataframe_transformers import (
    DuplicateValueTransformer,
    ColumnNameTransformer,
)
from etl.validators.column_validators import (
    ColumnNameValidator,
    DuplicateValueValidator,
)

from etl.orchestrators.nwea import NWEAAssessmentOrchestrator

# load the environment variables
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)
base_dir = os.path.dirname(os.path.dirname(current_dir))
env_path = os.path.join(base_dir, ".env")
load_dotenv(env_path)

# NWEA Assessment API Connector
connector = NWEAAssessmentConnector(
    url="https://api.nwea.org/test-results/v1/growth",
    params={"school-bid": "2c195342-9ea0-410c-afb5-0713dd0e6e0a"},
    bid="2c195342-9ea0-410c-afb5-0713dd0e6e0a",
    max_date=pd.to_datetime("2023-09-23"),
    return_data=True,
)

# Transformation steps for ETL pipeline
json_1 = JSONColumnNormalizer(
    col="lexile",
    new_col_names=None,
    drop_original_col=True,
    col_mapping={
        "score": "LEXILE_SCORE",
        "min": "LEXILE_MIN",
        "max": "LEXILE_MAX",
        "range": "LEXILE_RANGE",
    },
    explode_col=False,
    missing_cols_mapping={
        "LEXILE_SCORE": None,
        "LEXILE_MIN": None,
        "LEXILE_MAX": None,
        "LEXILE_RANGE": None,
    },
)

json_2 = JSONColumnNormalizer(
    col="instructionalAreas",
    drop_original_col=True,
    new_col_names=[
        "INSTRUCTIONAL_AREA_BID",
        "INSTRUCTIONAL_AREA_NAME",
        "INSTRUCTIONAL_AREA_SCORE",
        "INSTRUCTIONAL_AREA_STD_ERR",
        "INSTRUCTIONAL_AREA_LOW",
        "INSTRUCTIONAL_AREA_HIGH",
    ],
    explode_col=True,
    missing_cols_mapping={
        "INSTRUCTIONAL_AREA_BID": np.nan,
        "INSTRUCTIONAL_AREA_NAME": None,
        "INSTRUCTIONAL_AREA_SCORE": np.nan,
        "INSTRUCTIONAL_AREA_STD_ERR": np.nan,
        "INSTRUCTIONAL_AREA_LOW": np.nan,
        "INSTRUCTIONAL_AREA_HIGH": np.nan,
    },
)

json_3 = JSONColumnNormalizer(
    col="quantile",
    new_col_names=None,
    drop_original_col=True,
    col_mapping={
        "score": "QUANTILE_SCORE",
        "minimum": "QUANTILE_MIN",
        "maximum": "QUANTILE_MAX",
        "range": "QUANTILE_RANGE",
        "original": "QUANTILE_ORIGINAL",
    },
)

json_4 = JSONColumnNormalizer(
    col="norms",
    new_col_names=[
        "NORMS_PERCENTILE",
        "NORMS_REFERENCE",
        "NORMS_TYPE",
    ],
    drop_original_col=True,
    explode_col=True,
    missing_cols_mapping={
        "NORMS_PERCENTILE": np.nan,
        "NORMS_REFERENCE": None,
        "NORMS_TYPE": None,
    },
)

json_5 = JSONColumnNormalizer(
    col="items",
    new_col_names=None,
    drop_original_col=True,
    col_mapping={
        "shown": "ITEMS_SHOWN",
        "correct": "ITEMS_CORRECT",
        "total": "ITEMS_TOTAL",
    },
    explode_col=False,
    missing_cols_mapping={
        "ITEMS_SHOWN": np.nan,
        "ITEMS_CORRECT": np.nan,
        "ITEMS_TOTAL": np.nan,
    },
)

name_transformer = ColumnNameTransformer(
    col_mapping={
        "testResultBid": "TEST_RESULT_BID",
        "studentBid": "STUDENT_BID",
        "schoolBid": "SCHOOL_BID",
        "termBid": "TERM_BID",
        "subjectArea": "SUBJECT_AREA",
        "grade": "GRADE",
        "testName": "TEST_NAME",
        "testKey": "TEST_KEY",
        "testType": "TEST_TYPE",
        "growthEventYn": "GROWTH_EVENT_YN",
        "duration": "DURATION",
        "status": "STATUS",
        "rit": "RIT",
        "standardError": "STANDARD_ERROR",
        "ritScoreHigh": "RIT_SCORE_HIGH",
        "ritScoreLow": "RIT_SCORE_LOW",
        "responseDisengagedPercentage": "RESPONSE_DISENGAGED_PERCENTAGE",
        "impactOfDisengagement": "IMPACT_OF_DISENGAGEMENT",
        "administrationStartDateTime": "ADMINISTRATION_START_DATE_TIME",
        "administrationEndDateTime": "ADMINISTRATION_END_DATE_TIME",
        "modifiedDateTime": "MODIFIED_DATE_TIME",
        "ITEMS_SHOWN": "ITEMS_SHOWN",
        "ITEMS_CORRECT": "ITEMS_CORRECT",
        "ITEMS_TOTAL": "ITEMS_TOTAL",
        "NORMS_PERCENTILE": "NORMS_PERCENTILE",
        "QUANTILE_SCORE": "QUANTILE_SCORE",
        "QUANTILE_MAX": "QUANTILE_MAX",
        "QUANTILE_MIN": "QUANTILE_MIN",
        "QUANTILE_RANGE": "QUANTILE_RANGE",
        "QUANTILE_ORIGINAL": "QUANTILE_ORIGINAL",
        "INSTRUCTIONAL_AREA_BID": "INSTRUCTIONAL_AREA_BID",
        "INSTRUCTIONAL_AREA_NAME": "INSTRUCTIONAL_AREA_NAME",
        "INSTRUCTIONAL_AREA_SCORE": "INSTRUCTIONAL_AREA_SCORE",
        "INSTRUCTIONAL_AREA_STD_ERR": "INSTRUCTIONAL_AREA_STD_ERR",
        "INSTRUCTIONAL_AREA_LOW": "INSTRUCTIONAL_AREA_LOW",
        "INSTRUCTIONAL_AREA_HIGH": "INSTRUCTIONAL_AREA_HIGH",
        "LEXILE_SCORE": "LEXILE_SCORE",
        "LEXILE_MIN": "LEXILE_MIN",
        "LEXILE_MAX": "LEXILE_MAX",
        "LEXILE_RANGE": "LEXILE_RANGE",
        "parentBid": "PARENT_SCHOOL_BID",
        "accommodations": "ACCOMMODATIONS",
    }
)

dupe_transformer = DuplicateValueTransformer(
    subset=["testResultBid", "modifiedDateTime", "INSTRUCTIONAL_AREA_BID"],
)

# validators for ETL pipeline
name_validator = ColumnNameValidator(
    required_columns=[
        "TEST_RESULT_BID",
        "STUDENT_BID",
        "SCHOOL_BID",
        "TERM_BID",
        "SUBJECT_AREA",
        "GRADE",
        "TEST_NAME",
        "TEST_KEY",
        "TEST_TYPE",
        "GROWTH_EVENT_YN",
        "DURATION",
        "STATUS",
        "RIT",
        "STANDARD_ERROR",
        "RIT_SCORE_HIGH",
        "RIT_SCORE_LOW",
        "RESPONSE_DISENGAGED_PERCENTAGE",
        "IMPACT_OF_DISENGAGEMENT",
        "ADMINISTRATION_START_DATE_TIME",
        "ADMINISTRATION_END_DATE_TIME",
        "MODIFIED_DATE_TIME",
        "ITEMS_SHOWN",
        "ITEMS_CORRECT",
        "ITEMS_TOTAL",
        "NORMS_PERCENTILE",
        "QUANTILE_SCORE",
        "QUANTILE_MAX",
        "QUANTILE_MIN",
        "QUANTILE_RANGE",
        "QUANTILE_ORIGINAL",
        "INSTRUCTIONAL_AREA_BID",
        "INSTRUCTIONAL_AREA_NAME",
        "INSTRUCTIONAL_AREA_SCORE",
        "INSTRUCTIONAL_AREA_STD_ERR",
        "INSTRUCTIONAL_AREA_LOW",
        "INSTRUCTIONAL_AREA_HIGH",
        "LEXILE_SCORE",
        "LEXILE_MIN",
        "LEXILE_MAX",
        "LEXILE_RANGE",
        "PARENT_SCHOOL_BID",
        "ACCOMMODATIONS",
    ]
)

dupe_validator = DuplicateValueValidator(
    subset=["TEST_RESULT_BID", "MODIFIED_DATE_TIME", "INSTRUCTIONAL_AREA_BID"],
)

# Exporter for ETL pipeline
exporter = FileExporter(file_name="test.csv", base_path="data/results")

if __name__ == "__main__":

    pipe = ETLPipeline(
        connector=connector,
        transformers=[
            json_1,
            json_2,
            json_3,
            json_4,
            json_5,
            dupe_transformer,
            name_transformer,
        ],
        validators=[name_validator, dupe_validator],
        exporter=exporter,
    )

    pipe.run()

    print(pipe.final_data_.head())

    bids = FileConnector(file_path = '../data/nwea.csv', method = 'pandas')
    cutoffs = FileConnector(file_path = '../data/parent_bids.csv', method = 'pands')

    """
    orchestrator = NWEAAssessmentOrchestrator(
        entities = bids,
        cutoffs = cutoffs,
        pipeline=pipe
    )
    """

    # definitely some problems with this, but will try and debug later
    # orchestrator.run()
