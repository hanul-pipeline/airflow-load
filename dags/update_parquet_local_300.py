from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# get variables
host_fastapi = Variable.get("host_fastapi")
port_fastapi = Variable.get("port_fastapi")
sensor_id = 300

# set execution date & time
# KST == GMT + 9
date = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%Y-%m-%d') }}"
time = "{{ (execution_date + macros.timedelta(hours=9)).strftime('%H') }}"

# default argument 설정
default_args = {
    'owner': 'hanul:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2023,11,8)
}

# dag settings
dag = DAG(
    f'update_parquet_local_{sensor_id}',
	default_args=default_args,
	tags=['load', 'parquet', 'local', 'curl'],
	max_active_runs=1,
	schedule_interval="5 * * * *")


# tasks
# start
start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

# curl
curl_update_local = BashOperator(
    task_id="curl.update.local",
    bash_command=f"curl 'http://{host_fastapi}:{port_fastapi}/parquet/local/{sensor_id}?date={date}&time={time}'",
    dag=dag
)

# send LINE notification
send_noti = BashOperator(
    task_id='send.noti',
    bash_command=f"""
    curl -X POST -H 'Authorization: Bearer jvQDUjm6vGgBqG1aA3Sm7rIhhHENN2PLP68CwRqjrMl' \
    -F 'message= {date} 일자 {time} 시 {sensor_id}번 센서 데이터: parquet 생성 과정에서 오류 발생' \
    https://notify-api.line.me/api/notify
    """,
    dag=dag,
	trigger_rule='one_failed'
)

# finish
finish = EmptyOperator(
	task_id = 'finish',
	dag = dag,
	trigger_rule='all_done'
)


# process
start >> curl_update_local >> [send_noti, finish]