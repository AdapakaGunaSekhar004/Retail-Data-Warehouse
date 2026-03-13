from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import csv

default_args = {
    "owner": "guna",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_raw_sales():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE raw_sales;")
    with open("/mnt/c/Users/adapa/OneDrive/Documents/Data-Warehouse/data/raw_sales.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO raw_sales (order_id, order_date, customer_id, customer_name,
                    product_id, product_name, category, quantity, unit_price, region)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row["order_id"], row["order_date"], row["customer_id"], row["customer_name"],
                row["product_id"], row["product_name"], row["category"],
                row["quantity"], row["unit_price"], row["region"]
            ))
    conn.commit()
    cursor.close()
    conn.close()
    print("Loaded raw sales data successfully")

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="Retail data warehouse pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["retail", "dbt", "postgres"],
) as dag:

    load_data = PythonOperator(
        task_id="load_raw_sales",
        python_callable=load_raw_sales,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /mnt/c/Users/adapa/OneDrive/Documents/Data-Warehouse/retail_dw && source /home/adapa/airflow_venv/bin/activate && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /mnt/c/Users/adapa/OneDrive/Documents/Data-Warehouse/retail_dw && source /home/adapa/airflow_venv/bin/activate && dbt test",
    )

    load_data >> dbt_run >> dbt_test
