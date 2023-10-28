from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# execution date 사용
date = "{{execution_date.strftime('%Y-%m-%d')}}"

# default argument 설정
default_args = {
    'owner': 'hanul:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2022,1,20, 0,0,00)
}

# dag 설정
dag = DAG(
    'update_parquet_hdfs',
	default_args=default_args,
	tags=['tag_1','tag_2'],
	max_active_runs=1,
	schedule_interval="5 0 * * *")

# curl API 주소 수정필요
def gen_bash_task(name:str, dag):
    bash_task = BashOperator(
        task_id = name,
        bash_command = f"""
        curl -X POST -H "Content-Type: application/json" -d '{{"date":{date}}}' http://121.171.111.149:9000/remove
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
curl_remove_mysql =gen_bash_task('curl.remove_mysql', dag)

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
start >> curl_remove_mysql >> finish