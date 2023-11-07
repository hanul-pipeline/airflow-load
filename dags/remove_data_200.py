from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


# get variables
host_fastapi = Variable.get("host_fastapi")
port_fastapi = Variable.get("port_fastapi")
sensor_id = 200

# set execution date & time
date = "{{ (execution_date + macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

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
curl_check_data = BashOperator(
    task_id="curl.check.data",
    bash_command=f"curl 'http://{host_fastapi}:{port_fastapi}/check/{sensor_id}?date={date}'",
    dag=dag
)


def define_way(**kwargs):
    return_value = kwargs['task_instance'].xcom_pull(task_ids='curl.check.data', key='return_value')
    print(f">>>>>>> RETURN VALUE : {return_value}")

    if return_value == "PASS":
        return 'curl.remove.data'
    elif return_value == "FAIL":
        return 'send.noti.check'
    

branch_define_way = BranchPythonOperator(
    task_id='branch.define.way',
    provide_context=True,
    python_callable=define_way,
    dag=dag,
)


# curl
curl_remove_data = BashOperator(
    task_id="curl.remove.data",
    bash_command=f"curl 'http://{host_fastapi}:{port_fastapi}/remove/{sensor_id}?date={date}'",
    dag=dag
)

# send LINE notification
send_noti_check = BashOperator(
    task_id='send.noti.check',
    bash_command=f"""
    curl -X POST -H 'Authorization: Bearer jvQDUjm6vGgBqG1aA3Sm7rIhhHENN2PLP68CwRqjrMl' \
    -F 'message= {date} 일자 {sensor_id}번 센서 데이터: 누적 개수 부족' \
    https://notify-api.line.me/api/notify
    """,
    dag=dag
)

# send LINE notification
send_noti_error = BashOperator(
    task_id='send.noti.error',
    bash_command=f"""
    curl -X POST -H 'Authorization: Bearer jvQDUjm6vGgBqG1aA3Sm7rIhhHENN2PLP68CwRqjrMl' \
    -F 'message= {date} 일자 {sensor_id}번 센서 데이터: 삭제 과정에서 오류 발생' \
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
start >> curl_check_data >> branch_define_way >> [curl_remove_data, send_noti_check]
send_noti_check >> finish
curl_remove_data >> [send_noti_error, finish]