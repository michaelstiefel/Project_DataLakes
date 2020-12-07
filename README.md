# Project Data Lake
This project makes use of Spark to bring data from the startup Sparkify
into a schema which allows for analytical queries. To this end, an ETL pipeline loads data from S3 buckets, brings them into a Star schema and stores them back on
an S3 bucket.

## Overview of files

1. etl.py
This file creates a spark session object, then loads the transactional databases from the S3 bucket. It then transforms this data into a star schema and stores
them back in an S3 object.



## How to use this repo

1. Add information in dl.cfg to connect to the AWS S3 bucket.

2. Run etl.py



## Database schema

Our Star schema centers
around the fact table songplays which contains the information on the events
in the Sparkify app - each song played by a user. To this end, it contains
information on the event (time, sessionid), the user (userid, level),
the artist (artist_id), the song (song_id) and some metainformation on the
event (user_agent, location).

The dimension tables are split into different tables for artists, songs,
users and times which can be connected to the songplays table via the
artist_id, the song_id, the user_id or the timestamp. Those tables allow
to filter straightforward for information related to those dimensions and they
do not contain any duplicates (for users, for example).
