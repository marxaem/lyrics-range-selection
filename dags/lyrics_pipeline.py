import os
import sys
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from google.cloud import bigquery
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

sys.path.insert(0, "/home/airflow/gcs/plugins/")
from utils.extraction import Extraction

# credentials
SPOTIFY_CID = Variable.get('spotify_cid')
SPOTIFY_SECRET = Variable.get('spotify_secret')
PLAYLIST_URL = Variable.get('playlist_url')
LIMIT = int(Variable.get('playlist_limit'))
PROJECT = Variable.get('project_id')
DATASET = Variable.get('dataset')

default_args = {
        "email_on_failure": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    }

with DAG(
    dag_id="lyrics_summarisation",
    default_args=default_args,
    description="A lyrics ETL pipeline",
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    spotify_creds = SpotifyClientCredentials(client_id=SPOTIFY_CID, client_secret=SPOTIFY_SECRET)
    spotify = spotipy.Spotify(client_credentials_manager=spotify_creds)
    client = bigquery.Client(project=PROJECT)

    extractor = Extraction(spotify, client)
    table_tophits = ".".join([PROJECT, DATASET, 'tophits'])
    table_lyrics = ".".join([PROJECT, DATASET, 'lyrics'])

    def extract_tophits(link, limit, execution_timestamp, **kwargs):
        ti = kwargs['ti']
        raw_tophits, to_ingest_tophits = extractor.get_playlist_songs(link, limit, execution_timestamp)
        ti.xcom_push(key='raw_tophits', value=raw_tophits)
        ti.xcom_push(key='to_ingest_tophits', value=to_ingest_tophits)
    
    def ingest_tophits(**kwargs):
        ti = kwargs['ti']
        to_ingest_tophits = ti.xcom_pull(key='to_ingest_tophits', task_ids='extract_tophits_task')
        extractor.ingest_into_bigquery(table_tophits, to_ingest_tophits)

    def extract_lyrics(**kwargs):
        ti = kwargs['ti']
        raw_tophits = ti.xcom_pull(key='raw_tophits', task_ids='extract_tophits_task')
        to_ingest_lyrics = extractor.get_playlist_lyrics(raw_tophits)
        ti.xcom_push(key='to_ingest_lyrics', value=to_ingest_lyrics)

    def ingest_lyrics(**kwargs):
        ti = kwargs['ti']
        to_ingest_lyrics = ti.xcom_pull(key='to_ingest_lyrics', task_ids='extract_lyrics_task')
        extractor.ingest_into_bigquery(table_lyrics, to_ingest_lyrics)

    extract_tophits_task = PythonOperator(
        task_id='extract_tophits_task',
        python_callable=extract_tophits,
        op_kwargs={'link': PLAYLIST_URL, 'limit': LIMIT, 'execution_timestamp': '{{ ts }}'},
        provide_context=True
    )

    ingest_tophits_task = PythonOperator(
        task_id='ingest_tophits_task',
        python_callable=ingest_tophits,
        provide_context=True
    )

    extract_lyrics_task = PythonOperator(
        task_id='extract_lyrics_task',
        python_callable=extract_lyrics,
        provide_context=True
    )

    ingest_lyrics_task = PythonOperator(
        task_id='ingest_lyrics_task',
        python_callable=ingest_lyrics,
        provide_context=True
    )

    extract_tophits_task >> ingest_tophits_task >> extract_lyrics_task >> ingest_lyrics_task