# Introduction 
This project provides a codebase for ETL processes started by the Data Dogs team.

The codebase consists of two parts:
 - A composable ETL pipeline system that enables the chaining of the following ETL steps:
    - Data connections:  file, database, API
    - Data transformations: exploding json, 
    - Data validations:  data types, duplicate values,
    - Data exporting:  save records to a database, file system, etc

This is located in the `etl` folder.

 - A collection of scripts that were written rather quickly to get ETL jobs done ASAP (ie, nwea data).

 These are located in the `scripts` folder.  

 The code in the `src` folder is meant to represent "future state", the code in the `scripts` folder is what's currently run in production.

# People :People
The primary maintainers of this repository are the following:

| Person | Role in Organization | Role in Repo |
|----------|----------|----------|
| Jonathan Bechtel | Data Team Lead | Admin |
| Sanjay Narajayan | Head DBA  | Admin |

Please contact either of them for assistance, questions, or help with pull requests.

# Current ETL Jobs
The following jobs are run from this repository

| Data Source | Location in Codebase | Frequency | Location |
|----------|----------|----------|--------|
| NWEA Assessments | scripts/nwea/assessments.py | Weekly | 10.120.60.242 |
| NWEA Students | scripts/nwea/students.py  | Monthly | 10.120.60.242 |

# Getting Started

If you have questions about how to run or setup the code in this repository, please send a message to jonathan bechtel at jonathan.b@fevtutor.com to get help.

**To run scripts to pull nwea data:**

1). Obtain the `.env` file to load environment variables for NWEA API data and put in the directory `scripts/nwea`
2). Run the command `pip install -r requirements.txt`

**For Assessments Data:**
 - Obtain the `csv` file called `parent_bids.csv` and place it in the directory `scripts/nwea/data`
 - Obtain the `csv` file called `master_bids_list.csv` and place it in the directory `scripts/nwea/data`
 - Run the command `python scripts/nwea/assessments.py`


**For Students Data:**
 - Obtain the `csv` file called `current_students.csv` and place it in the directory `scripts/nwea/data`
 - Run the command `python scripts/nwea/students.py`

# Roadmap :Roadmap
Current priorities for this codebase are:  

 - Fully automate scheduling of scripts on a daily basis:
    - Remove the need to generate csv files and connect directly to database for updates
    - Automatically export records for each school BID to database and omit the creation of file sources

# Building ETL Pipelines From ETL Codebase

The `etl` folder is the desired *future* state of our ETL systems, and has the intended behavior.

A `pipeline` consists of a `connector`, `transformers`, `validators` and `exporters`, that you can use to run an ETL job on a single instance of an entity.  Like 1 school in a school system.

Their initiation works like this:

```
from src.pipelines import ETLPipeline
from src.transformers import JSONColumnNormalizer
from src.validators import ColumnNameValidator
from src.loader import FileLoader
pipeline = ETLPipeline(
    connector = NWEAAssessmentConnector(..args..),
    transformers = [JSONColumnNormalizer(..args..)],
    validators = [ColumnNameValidator(..args..)],
    loader = FileLoader(..args..))
pipeline.run()
```

A pipeline is then passed into an `orchestrator` which contains all the necessary meta information to execute a pipeline for all entities in a data source.

Example usage looks like this:  

```
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
```