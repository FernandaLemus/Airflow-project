from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

#################################################################################################
#################################################################################################
#################################################################################################
######         The following DAG performs the following functions:                          #####
######                                                                                      #####
###### 1. Loads long_data and song_data from S3 to RedShift through StageToRedshiftOperator #####
###### 2. Uses the  LoadFactOperator to create a Fact table in Redshift                     #####
###### 3. Uses the LoadDimensionOperator to create dimensions table in Redshift             #####
###### 4. Performs a data quality check on the Trips table in RedShift                      #####
#################################################################################################
#################################################################################################
#################################################################################################

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 3, 20),
    'depends_on_past': False,
    'retries' : 3,
    'retry_delay':300,
    'catchup_by_default': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id = 'redshift',
    aws_conn_id = 'aws_credentials',
    table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_key = 'log_data',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id = 'redshift',
    table = 'staging_songs',
    s3_bucket ='udacity-dend',
    s3_key = 'song_data',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = 'redshift',
    destination_table = 'songplays',
    query_fact = """(start_time, userid, level, songid,artistid, sessionid, location, user_agent)
                      SELECT
                        timestamp 'epoch' + se.ts * interval '0.001 seconds',
                        se.userId, se.level, ss.song_id, ss.artist_id,
                        se.sessionId, se.location, se.userAgent
                      FROM staging_events AS se LEFT JOIN staging_songs AS ss ON se.song = ss.title
                      WHERE se.page = 'NextSong' AND ss.song_id IS NOT NULL ;""",
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = 'redshift',
    destination_table = 'users',
    query_dimension = """ SELECT distinct se.userid, se.firstname, se.lastname, se.gender, se.level 
        FROM staging_events AS se 
        WHERE userid IS NOT NULL """,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = 'redshift',
    destination_table = 'songs',
    query_dimension = """ SELECT distinct ss.song_id, ss.title, ss.artist_id, ss.year,ss.duration
        FROM staging_songs AS ss WHERE ss.song_id IS NOT NULL """,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = 'redshift',
    destination_table = 'artists',
    query_dimension = """SELECT distinct ss.artist_id, ss.artist_name,
        ss.artist_location, ss.artist_latitude, ss.artist_longitude
        FROM staging_songs AS ss WHERE ss.artist_id IS NOT NULL """,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = 'redshift',
    destination_table = 'time',
    query_dimension = """SELECT timestamp 'epoch' + se.ts * interval '0.001 seconds',
            date_part(d, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(h, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(w, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(mon, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            date_part(y, timestamp 'epoch' + se.ts * interval '0.001 seconds'),
            extract (dayofweek FROM  timestamp 'epoch' + se.ts * interval '0.001 seconds')
            FROM staging_events AS se """,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = 'redshift',
    table_names = ['artists','songplays','users','songs','time']
    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

##################################################
################# DAG STRUCTURE ##################
##################################################

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
