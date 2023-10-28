from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# execution date 사용
date = "{{execution_date.strftime('%Y-%m-%d')}}"
time = "{{execution_date.strftime('%H')}}"

# default argument 설정
default_args = {
    'owner': 'hanul:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2022,1,20, 1,5,00)
}

# dag 설정
dag = DAG(
    'update_parquet_hdfs',
	default_args=default_args,
	tags=['tag_1','tag_2'],
	max_active_runs=1,
	schedule_interval="0 */2 * * *")

# curl API 주소 수정필요
def gen_bash_task(name:str, location_id:int, sensor_id:int, dag):
    bash_task = BashOperator(
        task_id = name,
        bash_command = f"""
        curl -X POST -H "Content-Type: application/json" -d '{{"location_id":{location_id}, "sensor_id":{sensor_id}, "date":{date}, "time":{time}}}' http://121.171.111.149:9000/parquet/hdfs/{sensor_id}
	    """,
        dag = dag
    )
    return bash_task

# start
start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

# curl
curl_update_hdfs_100 =gen_bash_task('curl.update.hdfs.100', 7, 100, dag)
curl_update_hdfs_200 =gen_bash_task('curl.update.hdfs.200', 8, 200, dag)
curl_update_hdfs_300 =gen_bash_task('curl.update.hdfs.300', 10, 300, dag)
curl_update_hdfs_400 =gen_bash_task('curl.update.hdfs.400', 11, 400, dag)
curl_update_hdfs_500 =gen_bash_task('curl.update.hdfs.500', 7, 500, dag)

# # LINE NOTI
# send_noti = BashOperator(
#     task_id='send.noti',
#     bash_command='''
#     curl -X POST -H 'Authorization: Bearer fxANtArqOzDWxjissz34JryOGhwONGhC1uMN8qc59Z3'
#                  -F '<MESSAGE>' 
#                  https://notify-api.line.me/api/notify
#     ''',
#     dag=dag,
# 	trigger_rule='one_failed'
# )

# finish
finish = EmptyOperator(
	task_id = 'finish',
	dag = dag,
	trigger_rule='all_done'
)

# process
start >> [curl_update_hdfs_100, curl_update_hdfs_200, curl_update_hdfs_300, curl_update_hdfs_400, curl_update_hdfs_500] >> finish