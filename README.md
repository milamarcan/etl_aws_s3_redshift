# ETL using AWS S3 and Redshift 
## Project overview
This repository is aimed to build an ETL pipeline using AWS tools S3 and Redshift 
that extracts data from JSON files located in S3, loads them into staging tables in Redshift and then transforms them into a final Redshift dimentional tables. In addition, data modeling was performed to ensure a smooth data flow through the pipeline.

### Assignment
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. 

## Overview of the files in the repository
- 'create_table.py': creates fact and dimension tables for the star schema in Redshift.
- 'etl.py' is where data loads from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
- 'sql_queries.py' is where defined SQL statements are, which will be imported into the two other files above.
- 'img' folder contains an image for the current file.

## Running the project
### Pre-requisites
- create a new virtual environment for the project (optional but highly recommended)
- run 'requirements.txt' file to install dependencies for the project (pip install -r requirements.txt)
- fill 'dwh.cfg' file with AWS data: 
   - details of Redshift cluster and its database
   - details of IAM role that has read access to S3

### How to run the project
- run 'create_tables.py' to create tables
- run 'etl.py' to execute ETL process
