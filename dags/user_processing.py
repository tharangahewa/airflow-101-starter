import json
from datetime import datetime

from airflow.models import DAG, TaskInstance, Param
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from pymongo import MongoClient


def process(ti: TaskInstance, params: Param):
    users = ti.xcom_pull(task_ids=['extract_user'])

    if not len(users) or 'results' not in users[0]:
        raise ValueError('Error')

    user = users[0]['results'][0]
    processed_user = {
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['login']['username'],
    }

    return processed_user


def load(ti):
    users = ti.xcom_pull(task_ids=['process_user'])
    hook = MongoHook(conn_id='mongo_oracdb')
    conn: MongoClient = hook.get_conn()
    conn.get_database('oracdb').get_collection('user').insert_one(document=users[0])


dag = DAG(dag_id='user_proc', schedule_interval='*/1 * * * *', start_date=datetime(2022, 1, 1), catchup=False)

dummy_task = DummyOperator(task_id='dummy', dag=dag)

extract_user = SimpleHttpOperator(task_id='extract_user',
                                  http_conn_id='user_api',
                                  endpoint='api/',
                                  method='GET',
                                  response_filter=lambda response: json.loads(response.text),
                                  log_response=True,
                                  dag=dag)
extract_user.set_upstream(dummy_task)

process_user = PythonOperator(task_id='process_user',
                              python_callable=process)
process_user.set_upstream(extract_user)

load_user = PythonOperator(task_id='load_user',
                           python_callable=load)
load_user.set_upstream(process_user)
