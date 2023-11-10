from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


# get variables
host_fastapi = Variable.get("host_fastapi")
port_fastapi = Variable.get("port_fastapi")
sensor_id = 400

# set execution date & time
date = "{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
time = "{{ (execution_date - macros.timedelta(hours=1)).strftime('%H') }}"

# default argument 설정
default_args = {
    'owner': 'hanul:1.0.0',
    'depends_on_past': True,
    'start_date': datetime(2023,10,23)
}

# dag settings
dag = DAG(
    f'remove_data_{sensor_id}',
	default_args=default_args,
	tags=['remove', 'data', 'mysql', 'curl'],
	max_active_runs=1,
	schedule_interval="5 * * * *")


# tasks
# start
start = EmptyOperator(
	task_id = 'start',
	dag=dag
)

# curl
curl_remove_data = BashOperator(
    task_id="curl.remove.data",
    bash_command=f"curl 'http://{host_fastapi}:{port_fastapi}/parquet/local/{sensor_id}?date={date}&time={time}'",
    dag=dag
)

# send LINE notification
send_noti = BashOperator(
    task_id='send.noti',
    bash_command='''
    curl -X POST -H 'Authorization: Bearer fxANtArqOzDWxjissz34JryOGhwONGhC1uMN8qc59Z3'
                 -F '<MESSAGE>' 
                 https://notify-api.line.me/api/notify
    ''',
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
start >> curl_remove_data >> [send_noti, finish]