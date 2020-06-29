"""
Download rides data from https://cycling.data.tfl.gov.uk/

"""
import logging
import json
from datetime import datetime as dt, timedelta as td

from airflow import DAG, configuration
from airflow.models import Variable, DagRun
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from fetch_data import upload_files
from merge_tables_sql import sql

# workaround to set smtp_password, because airflow.cfg is not work.
# configuration.conf.set('smtp', 'SMTP_PASSWORD', Variable.get('AIRFLOW_SMTP_PASSWORD'))

# defaults
EXECUTION_DATE = '{{ ds }}'
SCHEDULE_INTERVAL = "30 10 * * *"
DAG_NAME = 'dl_bike_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dt(2020, 1, 1),
    'email': '',#Variable.get('alert_analytics_mail'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': td(minutes=5),
    'execution_timeout': td(hours=1),
    'bigquery_conn_id': 'bigquery_default',
    'google_cloud_storage_conn_id': 'google_cloud_storage_default',
    'gcs_bucket': 'bikes_data_csv',
}

dag = DAG(dag_id=DAG_NAME,
          default_args=default_args,
          catchup=False, # =True if we need backfill past dates.
          schedule_interval=SCHEDULE_INTERVAL)
dag.doc_md = __doc__


with open('/home/airflow/gcs/dags/schema.json') as f:
    schema = json.load(f)

# Task1: download bike rides
def fetch_data(templates_dict, *args, **kwargs):
    """Logic, how to fetch  data and store to GCS->BQ"""

    number_files = upload_files(execution_date=templates_dict['EXECUTION_DATE'],
                                 google_cloud_storage_conn_id=default_args['google_cloud_storage_conn_id'],
                                 gcs_bucket=default_args['gcs_bucket'])
    return number_files

t1 = PythonOperator(
        pool='gcs_pool',
        task_id=f'upload_data_to_gcs',
        provide_context=True,
        python_callable=fetch_data,
        # op_kwargs={
        # },
        templates_dict={  # use this for arguments with templates
            'EXECUTION_DATE': EXECUTION_DATE,
        },
        dag=dag
    )

# branching: start -> branching: OR[true_task, false_task] -> finish
def branching_task(templates_dict, **kwargs):

    ti = kwargs.get('ti', None)
    rows_count = ti.xcom_pull(key=None,
                              task_ids='upload_data_to_gcs')  # get returned value from `start` task
    logging.info(f'total rows uploaded: {rows_count}')

    if rows_count > 0:
        logging.info('start process true_task')
        return 'gcs_to_bq'
    else:
        logging.info('start process false_task')
        return 'false_task'


branching = BranchPythonOperator(
    pool='gcs_bq_pool',
    task_id=f'branching',
    provide_context=True,
    python_callable=branching_task,
    templates_dict={  # use this for arguments with templates
    },
    dag=dag)

false_task = DummyOperator(
    task_id='false_task',
    dag=dag
)

# Task2: copy data from GCS files to BigQuery table
t2 = GoogleCloudStorageToBigQueryOperator(
    pool='gcs_bq_pool',
    task_id=f'gcs_to_bq',
    bucket=default_args['gcs_bucket'],
    source_objects=[
        f'{EXECUTION_DATE}/*.csv.gz'
    ],
    destination_project_dataset_table='platinum-trees-277807.bikes_data.cycle_hire{{ ds_nodash }}',
    source_format='CSV',
    skip_leading_rows=1,
    # autodetect=True,
    allow_quoted_newlines=True,
    schema_fields=schema,
    # write_disposition='WRITE_APPEND',  # append to the end of table
    write_disposition='WRITE_TRUNCATE',  # replace all data
    google_cloud_storage_conn_id=default_args['google_cloud_storage_conn_id'],
    dag=dag
)

# Task3: merge data into one table
t3 =  BigQueryOperator(
        pool='merge_pool',
        task_id='bq_merge_events',
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        allow_large_results=True,
        sql=sql,
        dag=dag
    )

# run
t1 >> branching >> [t2, false_task]
t2 >> t3
