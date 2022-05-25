from datetime import datetime

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from forms import get_forms
from forms_parser import parse_with_mapping
from yahoo import get_data_from_yahoo


def set_config(**kwargs):
    print("Получение конфиг файла")
    with open(f"/opt/airflow/configs/{kwargs['params']['config']}", 'r') as f:
        config = yaml.safe_load(f)

    print(config)
    v = Variable()

    for key, value in config.items():
        serialize = False

        if isinstance(v, dict):
            serialize = True

        v.set(key=key, value=value, serialize_json=serialize)


with DAG("dag_main", start_date=datetime(2022, 5, 21), schedule_interval="@daily", catchup=False) as dag:
    set_config_node = PythonOperator(
        task_id="set_config_node",
        dag=dag,
        provide_context=True,
        python_callable=set_config,
    )

    load_forms_node = PythonOperator(
        task_id="load_forms_node",
        dag=dag,
        provide_context=True,
        python_callable=get_forms,
    )

    parse_forms_node = PythonOperator(
        task_id="parse_forms_node",
        dag=dag,
        provide_context=True,
        python_callable=parse_with_mapping,
    )

    load_yahoo_node = PythonOperator(
        task_id="load_yahoo_node",
        dag=dag,
        provide_context=True,
        python_callable=get_data_from_yahoo,
    )

    set_config_node >> load_forms_node >> parse_forms_node >> load_yahoo_node
