# Data Warehouse Operating Manual (Under Construction)

The following document is designed to outline different operating procedures currently in place for the Data Warehouse.  It's meant to provide a high level guide to explain the what, why, how and when of data warehouse operations so other people can quickly grasp how it works.  

# Table of Contents

 1. Current installations
 2. Basic workflow
 3. Conventions:
    - Schemas
    - Naming
        - Tables:
            - Destination
            - Staging
        - Columns:
            - Destination
            - Staging
    - Data Validation
    - Data Connections
    - Indexes
    - Documentation
 4. Our release process
 5. Adding new data sources
 6. Team Roles
 7. Roadmap

# Current Installations

Here are some basic declarative facts about our current data warehouse setup:

 - Our data warehouse:  Oracle Autonomous Data Warehouse (ADW): insert link
 - Our warehouse installations:
    - Production:
        Staging database: some link
        Destination database: some link
    - POC:  
        Staging database: some link
        Destination database: some link

 - Our github repos:
    - ETL processes:  some link
    - DDL code for the Data Warehouse:  some link
 - Our UI based data connection tool:  Oracle Integration Connector (OIC): some link
 - Our enterprise resource diagram:  some link
 - Our data catalogue for public users:  some link
 - Our data catalogue for technical users:  some link

 # Basic Workflow

 The process diagram for data inflows into the data warehouse looks like this:

 ***** Some drawing placed here *****

 The basic steps are as follows:
  - We connect to data either via OIC (conventional sources) or via our ETL python code (unconventional sources)
  - OIC pipes data to the staging database in 4 hour increments, where a stored procedure is triggered to transfer data to the destination tables
  - After data leaves the staging database it is archived for historical record before it proceeds to get transformed into the destination tables

## Conventions
    
For our data modeling we follow a conventional star schema with the following characteristics:
    - Fact tables store numeric transactional data that can be aggregated
    - Fact tables are typically long and thin
    - Fact tables are typically a combination of numeric values and identifiers that can be linked out to dimension tables
    - Dimension tables store descriptors that can be joined to facts
    - Dimension tables are typically short and fat (de-normalized)
    - Dimension tables are mostly text based data, but may contain numeric data that describes the entity
    - Fact tables link to dimension tables via surrogate keys, which are integers generated inside the data warehouse and not dependent on any business logic

Here's an example schema, taken from NWEA data:

<! insert link to schema below>

## Naming

**For Destination Tables:**

Most tables created in the Data warehouse follow this naming convention:

{DATA_SOURCE}_{TABLE_TOPIC}_{FACT_OR_DIMENSION}

Table names should have the following characteristics

    - All uppercase letters
    - All words are separated by an undercase
    - First word of the table is the data source
    - Last word of the table is either `F` or `D`, depending on whether it's a Fact or Dimension table
    - middle word is the entity being described by the table.  

For example, for the NWEA data source all tables that come from it would begin with `NWEA`.  The nwea data source produces one fact table about assessments taken, one dimension table about students who take the test, and a separate dimension table about assessments.  

So the three table names would be:
- `NWEA_ASSESSMENT_F`
- `NWEA_ASSESSMENT_D`
- `NWEA_STUDENT_D`

**For Staging Tables:**

Staging tables follow a similar convention, that can be described like this:

`{DATA_SOURCE}_{TABLE_TOPIC}_STAGING`

Notes:
    - You do not use the `F` or `D` convention since they are not relevant for staging tables

To consider NWEA data:
    - It comes from two different API's:  assessment API and staging API.
    - Therefore, the staging tables would have the following names:
    - `NWEA_ASSESSMENT_STAGING`
    - `NWEA_STUDENT_STAGING`

The whole process can be described with the following diagram:


## Data Validation

Historically, our data validation setup has worked in the following way:

<! insert link to data validation schema here -- ask Keerthi to do it>

This diagram can be described in the following steps:

    - Have Keerthi or someone else fill this in 
    - Ditto
    - Ditto

New tables follow the current convention: