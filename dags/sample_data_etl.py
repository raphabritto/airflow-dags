# [START tutorial]
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mssql_operator import MsSqlOperator
from airflow.utils.dates import days_ago
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
# [END default_args]

# [START instantiate_dag]
dag = DAG(
    'sample_data_etl',
    default_args=default_args,
    description='Sample Data ETL',
    schedule_interval=None
)

t1 = MsSqlOperator(
    task_id='sample_data_precheck',
    mssql_conn_id='app_env_instanceid_mssql',
    sql='use myDB; select getDate()',
    dag=dag)


def sample_data_extract_func(**kwargs):
    print('Extracting Sample query...')
    mssql = MsSqlHook(mssql_conn_id='app_env_instanceid_mssql', schema='myDB')
    df = mssql.get_pandas_df(sql='select top 10 * from dbo.TableName')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df)
        
t2 = PythonOperator(
    task_id='sample_data_extract',
    python_callable=sample_data_extract_func,
    dag=dag
)

t1 >> t2    