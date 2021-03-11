# Project: Data Warehouse

> by Filipe Macedo 07 March 2020

## 1. Project Description

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

![](flow.png)

## 2. Datasets

We'll be working with two datasets that reside in S3. Here are the S3 links for each:

    Song data: s3://udacity-dend/song_data
    Log data: s3://udacity-dend/log_data

Log data json path: s3://udacity-dend/log_json_path.json

### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

    song_data/A/B/C/TRABCEI128F424C983.json
    song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

    log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json

## 3. Schema for Song Play Analysis

Using the song and event datasets, we'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

***Fact Table***

    songplay_table = records in event data associated with song plays i.e. records with page NextSong
        * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

***Dimension Tables***

    user_table = users in the app
        * user_id, first_name, last_name, gender, level
    song_table = songs in music database
        * song_id, title, artist_id, year, duration
    artist_table = artists in music database
        * artist_id, name, location, lattitude, longitude
    time_table = timestamps of records in songplays broken down into specific units
        * start_time, hour, day, week, month, year, weekday

## 4. Files

The project includes seven files:

- `create_tables.py` - creates all fact and dimension tables for the star schema in Redshift
- `etl.py` - loads data from S3 into staging tables on Redshift and then processes that data into analytics tables on Redshift
- `sql_queries.py` - defines SQL statements, which will be imported into the two other files above
- `dwh.cfg` - provides individual AWS credentails
- `redshift_create_cluster.ipynb` - creates individual Redshift cluster and updates `dwh.cf` with new DWH_ENDPOINT and DWH_ROLE_ARN values
- `test.ipynb` - where you can run some SQL querys to test de if the data was stored correctly
- `log_json_path.json` - if the JSON data objects don't correspond directly to column names, you can use a JSONPaths file to map the JSON elements to columns. The order doesn't matter in the JSON source data, but the order of the JSONPaths file expressions must match the column order. 
## 5. ETL Pipeline

1. Run the `redshift_create_cluster.ipynp` jupyter notebook to launch a redshift cluster and create an IAM role that has read access to S3.
2. Add redshift database and IAM role info to `dwh.cfg`. 
3. In a terminal, run the command `python create_tables.py` to create the table schemas in your redshift database.
4. Again, run `python etl.py` in a terminal and run the analytic queries on your Redshift database to compare your results with the expected results.
5. Run the querys in `test.ipynb` to execute the tests.
6. Delete your redshift cluster when finished.