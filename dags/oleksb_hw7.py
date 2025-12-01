from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

CONN_ID = "goit_mysql_db"   # підключення, яке ти створив у Airflow UI

#FUNCTIONS

def pick_medal(**context):
    medal = random.choice(["Bronze", "Silver", "Gold"])
    print(f"Picked medal: {medal}")
    return medal

def branch_task(**context):
    medal = context["ti"].xcom_pull(task_ids="pick_medal")
    return f"calc_{medal}"

def generate_delay():
    #print("Sleeping for 35 seconds...")
    #time.sleep(35)
    print("Sleeping for 10 seconds...")
    time.sleep(10)

# ---------------------------
#   DAG definition
# ---------------------------

default_args = {
    "owner": "oleksb",
    "start_date": datetime(2024, 8, 18),
}

with DAG(
    dag_id="yourname_medals_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["oleksb_hw7"]
) as dag:

    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONN_ID,
        sql="""
        CREATE TABLE IF NOT EXISTS oleksb.hw_dag_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal_value = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    branch = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=branch_task
    )

    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=CONN_ID,
        sql="""
            INSERT INTO oleksb.hw_dag_results (medal_type, count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    )

    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=CONN_ID,
        sql="""
            INSERT INTO oleksb.hw_dag_results (medal_type, count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """
    )

    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=CONN_ID,
        sql="""
            INSERT INTO oleksb.hw_dag_results (medal_type, count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """
    )

    delay_task = PythonOperator(
        task_id="generate_delay",
        python_callable=generate_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONN_ID,
        sql="""
            SELECT TIMESTAMPDIFF(SECOND,
                (SELECT created_at FROM oleksb.hw_dag_results ORDER BY id DESC LIMIT 1),
                NOW()
            ) < 30;
        """,
        mode="poke",
        poke_interval=5,
        timeout=20,
    )

    # DEPENDENCIES

    create_table >> pick_medal_value >> branch
    branch >> [calc_Bronze, calc_Silver, calc_Gold] >> delay_task >> check_for_correctness
