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

    # Recreate table with new schema
    cursor.execute("DROP TABLE IF EXISTS raw_sales;")
    cursor.execute("""
        CREATE TABLE raw_sales (
            invoiceno    VARCHAR(20),
            invoicedate  VARCHAR(20),
            invoicetime  VARCHAR(20),
            stockcode    VARCHAR(20),
            description  TEXT,
            quantity     VARCHAR(20),
            unitprice    VARCHAR(20),
            totalsale    VARCHAR(20),
            customerid   VARCHAR(20),
            country      VARCHAR(100),
            yearmonth    VARCHAR(20),
            isreturn     VARCHAR(10)
        );
    """)

    with open("/mnt/c/Users/adapa/OneDrive/Documents/Data-Warehouse/data/raw_sales.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append((
                row["InvoiceNo"],
                row["InvoiceDate"],
                row["InvoiceTime"],
                row["StockCode"],
                row["Description"],
                row["Quantity"],
                row["UnitPrice"],
                row["TotalSale"],
                row["CustomerID"],
                row["Country"],
                row["YearMonth"],
                row["IsReturn"],
            ))
            if len(batch) == 1000:
                cursor.executemany("""
                    INSERT INTO raw_sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, batch)
                batch = []
        if batch:
            cursor.executemany("""
                INSERT INTO raw_sales VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, batch)

    conn.commit()
    cursor.close()
    conn.close()
    print("Loaded 50,000 raw sales records successfully")

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    description="Retail data warehouse ELT pipeline",
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
