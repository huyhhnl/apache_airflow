from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from google_drive_downloader import GoogleDriveDownloader as gdd
from config import *

from datetime import datetime
import os

default_args = {
    'start_date': datetime(2023,9,19) 
} 
# download file từ google driver
def download_question_file():
    gdd.download_file_from_google_drive(file_id=QUESTION_DRIVER_ID,
                                        dest_path=QUESTION_FILE_PATH, showsize=True, overwrite=True)

def download_answer_file():
    gdd.download_file_from_google_drive(file_id=ANSWER_DRIVER_ID,
                                        dest_path=ANSWER_FILE_PATH, showsize=True, overwrite=True)
# tạo điều kiện: nếu file đã tồn tại thì chuyển đến task end, chưa thì chuyển đến task clean_file đề loại bỏ các file không cân thiết
def branch_file_exist():
    if os.path.exists(QUESTION_FILE_PATH) and os.path.exists(ANSWER_FILE_PATH):
        return 'end'
    return 'clear_file'

with DAG('project', schedule_interval='@daily',
         default_args=default_args, catchup=False ) as dag:
    # tạo 2 task start và end
    start = DummyOperator(task_id = 'start')
    end = DummyOperator(task_id = 'end')
    # task clear
    clear_file = BashOperator(task_id = 'clear_file',
                              bash_command=f'rm -f {QUESTION_FILE_PATH} && rm -f {ANSWER_FILE_PATH}')

    # task download file
    download_question_file_task = PythonOperator(task_id = 'download_question_file_task',
                                                python_callable=download_question_file)
    download_answer_file_task = PythonOperator(task_id = 'download_answer_file_task',
                                              python_callable=download_answer_file)

    # task điều kiện
    branching = BranchPythonOperator(task_id = 'branching',
                                     python_callable=branch_file_exist,
                                     provide_context = True,
                                     dag=dag)
    # tải dữ liệu đã tải về từ google driver vào mongodb
    import_questions_mongo = BashOperator(
        task_id = ' import_questions_mongo',
        bash_command=f'mongoimport --type csv -d airflow -c questions --headerline --drop {QUESTION_FILE_PATH}'
    )
    import_answers_mongo = BashOperator(
        task_id = ' import_answers_mongo',
        bash_command=f'mongoimport --type csv -d airflow -c answers --headerline --drop {ANSWER_FILE_PATH}'
    )
    # tải data sau khi đã xử lý với spark vào mongodb
    import_output_mongo = BashOperator(
        task_id = 'import_output_mongo',
        bash_command=f'mongoimport --type csv -d airflow -c output --headerline --drop {SPARK_OUTPUT_PATH}/*.csv'
    )
    # task spark để đọc data từ mongodb và xử lý theo yêu cầu
    spark_process = SparkSubmitOperator(
        task_id = 'spark_process',
        conn_id = 'spark_local',
        packages = 'org.mongodb.spark:mongo-spark-connector_2.12:10.2.0',
        application = SPARK_PYTHON_FILE_PATH,
        dag=dag
    )
    # thiết lập DAG
    start.set_downstream(branching)
    branching.set_downstream(clear_file)
    branching.set_downstream(end)
    clear_file.set_downstream([download_question_file_task, download_answer_file_task])
    download_question_file_task.set_downstream(import_questions_mongo)
    download_answer_file_task.set_downstream(import_answers_mongo)
    spark_process.set_upstream([import_questions_mongo, import_answers_mongo])
    spark_process.set_downstream(import_output_mongo)
    import_output_mongo.set_downstream(end)

    # start>>branching>>[clear_file, end]
    # clear_file>>[download_question_file_task, download_answer_file_task]
    # download_question_file_task>>import_questions_mongo
    # download_answer_file_task>>import_answers_mongo
    # [import_questions_mongo, import_answers_mongo]>>spark_process>>import_output_mongo>>end
