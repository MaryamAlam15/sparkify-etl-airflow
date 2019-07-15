## Summary
 A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new
 music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. 
 Currently, they don't have an easy way to query their data which resides in S3, in a directory of JSON logs on user 
 activity on the app, as well as a directory with JSON metadata on the songs in their app.
 
#### So, to resolve this issue:
In this project, 
   - data modeling is implemented with AWS Redshift.
   - staging tables are creating in Redshift to load data from data files.
   - an ETL pipeline is built using Airflow DAGs which will transform data from staging tables to dimension and fact tables using "star" schema.
   
   
### Staging Tables:
   - staging_events: reads from events logs data files.
   - staging_songs: reads from songs data files.
  
### Dimension Tables:
   - users: contains users in the music app.
   - songs: contains songs in the database.
   - artists: contains artists in the database.
   - time: timestamp of records in `songplays` broken down into specific units.
   
### Fact Table:
   - songplays: records in the log data associated with song plays.
   
### Airflow DAG to create and process ETL Pipeline:
   - transfers data from s3 bucket into the AWS Redshift tables using SQL and Python.
   
### How to run:
   - run command to install requirements.
        > pip install -r requirements.txt
   - then set `AIRFLOW_HOME` directory:
        > export AIRFLOW_HOME=`pwd`/airflow
   - Initialize the Airflow DB
        > airflow initdb
   - run airflow schedular
        > airflow schedular
   - run airflow web server
        > airflow webserver
   - open ``http://0.0.0.0:8080/admin/`` your DAG will be listed.
        - the schedule time should be set to every hour.
        - to run in manually, click trigger button.