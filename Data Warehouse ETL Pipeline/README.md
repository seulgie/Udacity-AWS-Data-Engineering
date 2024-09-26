# Sparkify Data Warehouse ETL Pipeline

## Project Overview

This project builds a data warehouse for Sparkify, a music streaming startup, to help them move their processes and data to the cloud. 
Their data is stored in Amazon S3 in JSON format, and the goal is to build an ETL pipeline that extracts the data from S3, stages it in Amazon Redshift, 
and transforms it into a star schema for their analytics team to query.

## Purpose

Sparkify's analytics team wants to gain insights into what songs their users are listening to. 
The star schema in this project is designed to optimize queries for analyzing song plays in Sparkify's app.
By setting up database tables in Redshift, this pipeline allows Sparkify to run analytics on user activity and song information efficiently.

## Datasets

Two datasets are stored in S3:
1. **Song data**: Metadata about the songs and artists available on the Sparkify platform.
   - S3 Path: `s3://udacity-dend/song_data`
   
2. **Log data**: App activity logs that capture user interactions with the Sparkify platform.
   - S3 Path: `s3://udacity-dend/log_data`
   - Log JSON Path: `s3://udacity-dend/log_json_path.json`

## Database Schema Design

The database is structured as a **star schema**, optimized for song play analysis. It consists of one **fact table** and four **dimension tables**:

### Fact Table
- **`songplays`**: Records of song plays (event data from logs) with the following columns:
  - `songplay_id`: Primary key
  - `start_time`: Timestamp of the song play
  - `user_id`: ID of the user who played the song
  - `level`: Subscription level (free/paid)
  - `song_id`: ID of the song played
  - `artist_id`: ID of the artist
  - `session_id`: ID of the user session
  - `location`: Location of the user
  - `user_agent`: User agent string

### Dimension Tables
- **`users`**: Information about users of the app:
  - `user_id`: Primary key
  - `first_name`: User's first name
  - `last_name`: User's last name
  - `gender`: User's gender
  - `level`: Subscription level (free/paid)
  
- **`songs`**: Information about songs:
  - `song_id`: Primary key
  - `title`: Song title
  - `artist_id`: ID of the artist
  - `year`: Release year of the song
  - `duration`: Duration of the song
  
- **`artists`**: Information about artists:
  - `artist_id`: Primary key
  - `name`: Artist's name
  - `location`: Location of the artist
  - `latitude`: Latitude of the artist’s location
  - `longitude`: Longitude of the artist’s location
  
- **`time`**: Timestamps of songplays broken down into different units:
  - `start_time`: Primary key
  - `hour`: Hour of the day
  - `day`: Day of the month
  - `week`: Week of the year
  - `month`: Month
  - `year`: Year
  - `weekday`: Name of the weekday

## ETL Pipeline

The ETL pipeline performs the following steps:

1. **Extract data from S3**:
   - Copy raw JSON files (log data and song data) from S3 to Amazon Redshift staging tables.

2. **Transform data**:
   - Filter, transform, and join the data in the staging tables.

3. **Load into final tables**:
   - Insert the transformed data into fact and dimension tables for analytics.

### ETL Steps:
- **Staging Tables**:
  - `staging_events`: Stores raw log data from S3.
  - `staging_songs`: Stores raw song metadata from S3.
  
- **Transformation & Loading**:
  - Data is extracted from the staging tables and inserted into the appropriate dimension and fact tables. For example, data from the `staging_events` and `staging_songs` tables is joined to populate the `songplays` fact table.

## Project Files

This project consists of the following files:

1. **`sql_queries.py`**:
   - Contains SQL queries for creating, dropping, copying data to staging tables, and inserting data into fact and dimension tables.

2. **`create_tables.py`**:
   - Script to create the database schema in Redshift. It connects to the Redshift cluster, drops existing tables (if any), and creates the required staging and analytics tables.
   
3. **`etl.py`**:
   - This script implements the ETL process. It extracts data from S3 into Redshift staging tables, transforms the data, and loads it into the fact and dimension tables.

4. **`dwh.cfg`**:
   - Configuration file containing AWS credentials, Redshift cluster details, and S3 paths.

5. **`README.md`**:
   - The file you are reading. It provides an overview of the project, schema design, and steps to execute the ETL pipeline.

## How to Run the Project

1. **Set up a Redshift Cluster**:
   - Create a Redshift cluster in AWS and configure an IAM role with access to S3.

2. **Update `dwh.cfg`**:
   - Update the `dwh.cfg` file with your Redshift cluster connection details and the ARN of your IAM role.

3. **Run the pipeline**:
   - Execute `create_tables.py` to create the staging and analytics tables in Redshift.
   - Execute `etl.py` to load data from S3 into the staging tables, transform the data, and load it into the fact and dimension tables.

4. **Test the ETL process**:
   - Use the AWS Redshift query editor to run SQL queries on your fact and dimension tables to verify the data has been loaded correctly.

## Cleanup

After running the ETL process and ensuring everything is working correctly, make sure to delete the Redshift cluster to avoid incurring unnecessary costs.
