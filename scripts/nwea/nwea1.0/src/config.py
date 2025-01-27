import logging

from datetime import datetime
import numpy as np

data_cols_config = {
    'testResultBid':'TEST_RESULT_BID',
    'studentBid':'STUDENT_BID',
    'schoolBid':'SCHOOL_BID',
    'termBid':'TERM_BID',
    'subjectArea':'SUBJECT_AREA',
    'grade':'GRADE',
    'testName':'TEST_NAME',
    'testKey':'TEST_KEY',
    'testType':'TEST_TYPE',
    'growthEventYn':'GROWTH_EVENT_YN',
    'duration':'DURATION',
    'status':'STATUS',
    'rit':'RIT',
    'standardError':'STANDARD_ERROR',
    'ritScoreHigh':'RIT_SCORE_HIGH',
    'ritScoreLow':'RIT_SCORE_LOW',
    'responseDisengagedPercentage':'RESPONSE_DISENGAGED_PERCENTAGE',
    'impactOfDisengagement':'IMPACT_OF_DISENGAGEMENT',
    'administrationStartDateTime':'ADMINISTRATION_START_DATE_TIME',
    'administrationEndDateTime':'ADMINISTRATION_END_DATE_TIME',
    'modifiedDateTime':'MODIFIED_DATE_TIME',
    'ITEMS_SHOWN':'ITEMS_SHOWN',
    'ITEMS_CORRECT':'ITEMS_CORRECT',
    'ITEMS_TOTAL':'ITEMS_TOTAL',
    'NORMS_PERCENTILE':'NORMS_PERCENTILE',
    'QUANTILE_SCORE':'QUANTILE_SCORE',
    'QUANTILE_MAX':'QUANTILE_MAX',
    'QUANTILE_MIN':'QUANTILE_MIN',
    'QUANTILE_RANGE':'QUANTILE_RANGE',
    'QUANTILE_ORIGINAL':'QUANTILE_ORIGINAL',
    'INSTRUCTIONAL_AREA_BID':'INSTRUCTIONAL_AREA_BID',
    'INSTRUCTIONAL_AREA_NAME':'INSTRUCTIONAL_AREA_NAME',
    'INSTRUCTIONAL_AREA_SCORE':'INSTRUCTIONAL_AREA_SCORE',
    'INSTRUCTIONAL_AREA_STD_ERR':'INSTRUCTIONAL_AREA_STD_ERR',
    'INSTRUCTIONAL_AREA_LOW':'INSTRUCTIONAL_AREA_LOW',
    'INSTRUCTIONAL_AREA_HIGH':'INSTRUCTIONAL_AREA_HIGH',
    'LEXILE_SCORE':'LEXILE_SCORE',
    'LEXILE_MIN':'LEXILE_MIN',
    'LEXILE_MAX':'LEXILE_MAX',
    'LEXILE_RANGE':'LEXILE_RANGE',
    'parentBid':'PARENT_SCHOOL_BID',
    'accommodations': 'ACCOMMODATIONS'
}

student_cols_config = {
    'dateOfBirth': 'DATE_OF_BIRTH',
    'districtBid': 'DISTRICT_BID',
    'ethnicity': 'ETHNICITY',
    'ethnicityCustom': 'ETHNICITY_CUSTOM',
    'firstName': 'FIRST_NAME',
    'gender': 'GENDER',
    'grade': 'GRADE',
    'gradeCustom': 'GRADE_CUSTOM',
    'lastName': 'LAST_NAME',
    'middleName': 'MIDDLE_NAME',
    'schoolBid': 'SCHOOL_BID',
    'studentId': 'STUDENT_ID', 
    'stateStudentId': 'STATE_STUDENT_ID',
    'studentBid': 'STUDENT_BID'
}

dtype_config = {
    'TEST_RESULT_BID': 'str',
    'STUDENT_BID': 'str',
    'SCHOOL_BID': 'str',
    'TERM_BID': 'str',
    'SUBJECT_AREA': 'str',
    'GRADE': 'str',
    'TEST_NAME': 'str',
    'TEST_KEY': 'str',
    'TEST_TYPE': 'str',
    'GROWTH_EVENT_YN': 'str',
    'DURATION': 'float',
    'STATUS': 'str',
    'RIT': 'float',
    'STANDARD_ERROR': 'float',
    'RIT_SCORE_HIGH': 'float',
    'RIT_SCORE_LOW': 'float',
    'RESPONSE_DISENGAGED_PERCENTAGE': 'float',
    'IMPACT_OF_DISENGAGEMENT': 'str',
    'ADMINISTRATION_START_DATE_TIME': 'datetime64',
    'ADMINISTRATION_END_DATE_TIME': 'datetime64',
    'MODIFIED_DATE_TIME': 'datetime64',
    'ACCOMODATIONS_NAME': 'str',
    'ACCOMODATIONS_CATEGORY': 'str',
    'PARENT_BID': 'str',
    'ITEMS_SHOWN': 'float',
    'ITEMS_CORRECT': 'float',
    'ITEMS_TOTAL': 'float',
    'PERCENTILE': 'float',
    'REFERENCE': 'str',
    'TYPE': 'str',
    'QUANTILE_SCORE': 'float',
    'QUANTILE_MAX': 'float',
    'QUANTILE_MIN': 'float',
    'QUANTILE_RANGE': 'float',
    'QUANTILE_ORIGINAL': 'float',
    'INSTURCTIONAL_AREA_BID': 'str',
    'INSTRUCTIONAL_AREA_NAME': 'str',
    'INSTRUCTIONAL_AREA_SCORE': 'float',
    'INSTRUCTIONAL_AREA_STD_ERR': 'float',
    'INSTRUCTIONAL_AREA_LOW': 'float',
    'INSTRUCTIONAL_AREA_HIGH': 'float',
    'LEXILE_SCORE': 'float',
    'LEXILE_MIN': 'float',
    'LEXILE_MAX': 'float',
    'LEXILE_RANGE': 'float'
}

def setup_logging():
    """Setup logging for the project"""
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)