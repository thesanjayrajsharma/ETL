from sqlalchemy import VARCHAR, Float
import logging
from sqlalchemy.types import String, Float



col_rename={
            "testResultBid": "TEST_RESULT_BID",
            "studentBid": "STUDENT_BID",
            "schoolBid": "SCHOOL_BID",
            "subject": "SUBJECT_AREA",
            "grade": "GRADE",
            "testName": "TEST_NAME",
            "testKey": "TEST_KEY",
            "testType": "TEST_TYPE",
            "administrationDate": "ADMINISTRATION_START_DATE_TIME",
            "administrationEndDate": "ADMINISTRATION_END_DATE_TIME",
            "duration": "DURATION",
            "status": "STATUS",
            "rit": "RIT",
            "standardError": "STANDARD_ERROR",
            "ritScoreHigh": "RIT_SCORE_HIGH",
            "ritScoreLow": "RIT_SCORE_LOW",
            "responseDisengagedPercentage": "RESPONSE_DISENGAGED_PERCENTAGE",
            "impactOfDisengagement": "IMPACT_OF_DISENGAGEMENT",
            "growthEventYn": "GROWTH_EVENT_YN",
            "term.termBid": "TERM_BID",
            "lexile.score": "LEXILE_SCORE",
            "lexile.range": "LEXILE_RANGE",
            "lexile.min": "LEXILE_MIN",
            "lexile.max": "LEXILE_MAX",
            "items.shown": "ITEMS_SHOWN",
            "items.correct": "ITEMS_CORRECT",
            "items.total": "ITEMS_TOTAL",
            "accommodations": "ACCOMMODATIONS",
            "INSTRUCTIONAL_AREA_STD_ERR": "INSTRUCTIONAL_AREA_STD_ERR",
            "INSTRUCTIONAL_AREA_SCORE": "INSTRUCTIONAL_AREA_SCORE",
            "INSTRUCTIONAL_AREA_BID": "INSTRUCTIONAL_AREA_BID",
            "INSTRUCTIONAL_AREA_NAME": "INSTRUCTIONAL_AREA_NAME",
            "INSTRUCTIONAL_AREA_HIGH": "INSTRUCTIONAL_AREA_HIGH",
            "INSTRUCTIONAL_AREA_LOW": "INSTRUCTIONAL_AREA_LOW",
            "NORMS_PERCENTILE": "NORMS_PERCENTILE"
        }



data_type = {
    "ACCOMMODATIONS": String(),  # Use SQLAlchemy String type
    "INSTRUCTIONAL_AREA_HIGH": Float(),
    "INSTRUCTIONAL_AREA_LOW": Float(),
    "INSTRUCTIONAL_AREA_SCORE": Float(),
    "INSTRUCTIONAL_STD_ERR": Float(),
    "NORMS_PERCENTILE": Float(),
    "QUANTILE_ORIGINAL": String(),
    "RESPONSE_DISENGAGED_PERCENTAGE": Float(),
    "STANDARD_ERROR": Float(),
    "ADMINISTRATION_END_DATE_TIME": String(),
    "ADMINISTRATION_START_DATE_TIME": String(),
    "GRADE": String(),
    "INSTRUCTIONAL_AREA_BID": String(),
    "INSTRUCTIONAL_AREA_NAME": String(),
    "LEXILE_MAX": String(),
    "LEXILE_MIN": String(),
    "LEXILE_RANGE": String(),
    "LEXILE_SCORE": String(),
    "SCHOOL_BID": String(),
    "STATUS": String(),
    "SUBJECT_AREA": String(),
    "ITEMS_SHOWN": Float(),
    "ITEMS_CORRECT": Float(),
    "ITEMS_TOTAL": Float(),

    "TEST_RESULT_BID": String(),  
    "STUDENT_BID": String(),      
    "TEST_NAME": String(),        
    "TEST_KEY": String(),        
    "TEST_TYPE": String(),        
    "DURATION": Float(),          
    "RIT": Float(),               
    "RIT_SCORE_HIGH": Float(),    
    "RIT_SCORE_LOW": Float(),     
    "IMPACT_OF_DISENGAGEMENT": Float(),  
    "GROWTH_EVENT_YN": String(),  
    "TERM_BID": String() 
}




