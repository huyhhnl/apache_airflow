from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
from pandas import json_normalize
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# dùng PostgresHook copy data từ file csv vào table users trong postgres
def _store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin WITH DELIMITER as ','",
        filename='/tmp/processed_user.csv'
    )
# Dùng Xcoms lấy dữ liệu từ task extract_user để xử lý 
def _process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user")
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'email': user['email'] })
    processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

# tạo DAG
with DAG('user_processing', start_date=datetime(2023,9,13), schedule_interval='@daily', catchup=False) as dag:
  # Tạo table users trong postgres với connect được thiết lập trong airflow  
  create_table = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres',
        sql = '''
                CREATE TABLE IF NOT EXISTS users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL PRIMARY KEY
                );
            '''
    )
    # Lấy dữ liệu có sẵn từ api nên kiểm tra xem api còn hoạt động không với connect đã được thiết lập trong airflow
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint=''
    )
    # extract data từ api
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    # task process user
    process_user = PythonOperator(
    task_id='process_user',
    python_callable=_process_user
    )
    # lưu trữ data vào postges db của airflow
    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user
    )
    create_table>>is_api_available>>extract_user>>process_user>>store_user

