from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
import emoji


def _choose_best_model(ti):
    results = ti.xcom_pull(key='model_accuracy', task_ids=[
        "training_model_A",
        "training_model_B",
        "training_model_C"
    ])

    print(emoji.emojize(":grinning_face_with_big_eyes:"))
    print(results)


def _training_model(ti):
    ti.xcom_push(key='model_accuracy', value=randint(1, 10))

with DAG("dag_main", start_date=datetime(2021, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    result = BashOperator(
        task_id="result",
        bash_command="echo 'result'"
    )

    no_result = BashOperator(
        task_id="no_result",
        bash_command="echo 'no_result'"
    )

    [training_model_A, training_model_B, training_model_C] >> choose_best_model
