import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

create_stg_products_sql = """
CREATE TABLE IF NOT EXISTS stg_products (
    id VARCHAR NOT NULL UNIQUE,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time timestamp
);
truncate stg_products;
"""

create_stg_inventory_sql = """
CREATE TABLE IF NOT EXISTS stg_inventory (
    productId VARCHAR NOT NULL,
    amount INTEGER,
    date DATE,
    processed_time timestamp
);
truncate stg_inventory;
"""

create_inventory_nums_categorized_recent_month_table_sql = """
CREATE TABLE IF NOT EXISTS inventory_nums_categorized_recent_month (
    amount INTEGER,
    category VARCHAR,
    date DATE,
    processed_time timestamp
);
truncate inventory_nums_categorized_recent_month;
"""

compute_inventory_nums_categorized_recent_month_sql = """
WITH INCRM AS (
    SELECT sum(stg_inventory.amount) AS amount, stg_products.category AS category, stg_inventory.date AS date
    FROM stg_products, stg_inventory
    WHERE stg_inventory.productId = stg_products.id
    AND
    date(stg_inventory.date) >= (CAST('{{ ts }}' AS date) - INTERVAL '1 month')
    GROUP BY stg_products.category, stg_inventory.date
)
INSERT INTO inventory_nums_categorized_recent_month(amount, category, date, processed_time)
SELECT INCRM.amount AS amount,
        INCRM.category AS category,
        INCRM.date AS date,
        '{{ ts }}' AS processed_time
FROM INCRM
"""

create_inventory_categorized_month_last_day_recent_two_years_table_sql = """
CREATE TABLE IF NOT EXISTS inventory_categorized_month_last_day_recent_two_years (
    amount INTEGER,
    category VARCHAR,
    date DATE,
    processed_time timestamp
);
truncate inventory_categorized_month_last_day_recent_two_years;
"""

compute_inventory_categorized_month_last_day_recent_two_years_sql = """
WITH ICMLDRTY AS (
    SELECT sum(stg_inventory.amount) AS amount, stg_products.category AS category, stg_inventory.date AS date
    FROM stg_products, stg_inventory
    WHERE stg_inventory.productId = stg_products.id
    AND
    date in (SELECT distinct(last_day_of_month) from dim_dates)
    AND
    date(stg_inventory.date) >= (CAST('{{ ts }}' AS date) - INTERVAL '2 years')
    GROUP BY stg_products.category, stg_inventory.date
)
INSERT INTO inventory_categorized_month_last_day_recent_two_years(amount, category, date, processed_time)
SELECT ICMLDRTY.amount AS amount,
        ICMLDRTY.category AS category,
        ICMLDRTY.date AS date,
        '{{ ts }}' AS processed_time
FROM ICMLDRTY
"""

def normalize_csv(ts, **kwargs):
    import csv
    source_filename = kwargs['source']
    target_filename = kwargs['target']
    header_skipped = False
    with open(source_filename, newline='') as source_file:
        with open(target_filename, "w", newline='') as target_file:
            reader = csv.reader(source_file, delimiter=',')
            writer = csv.writer(target_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                if not header_skipped:
                    header_skipped = True
                    continue
                row.append(ts)
                writer.writerow(row)
    return target_filename

def load_csv_to_postgres(table_name, **kwargs):
    csv_filepath = kwargs['csv_filepath']
    connecion = PostgresHook(postgres_conn_id=connection_id)
    connecion.bulk_load(table_name, csv_filepath)
    return table_name

with DAG(
    dag_id="goods_stats",
    start_date=datetime.datetime(2020, 1, 1),
    schedule_interval="@once",
    default_args=default_args,
    catchup=False,
) as dag:
    check_stg_products_csv_readiness = BashSensor(
        task_id="check_stg_products_csv_readiness",
        bash_command="""
            ls /data/raw/products_{{ ds }}.csv
        """,
    )

    check_stg_inventory_csv_readiness = BashSensor(
        task_id="check_stg_inventory_csv_readiness",
        bash_command="""
            ls /data/raw/inventory_{{ ds }}.csv
        """,
    )

    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql=create_stg_products_sql,
    )

    create_stg_inventory_table = PostgresOperator(
        task_id="create_inventory_products_table",
        postgres_conn_id=connection_id,
        sql=create_stg_inventory_sql,
    )

    create_inventory_nums_categorized_recent_month_table = PostgresOperator(
        task_id="create_inventory_nums_categorized_recent_month_table",
        postgres_conn_id=connection_id,
        sql=create_inventory_nums_categorized_recent_month_table_sql,
    )

    create_inventory_categorized_month_last_day_recent_two_years_table = PostgresOperator(
        task_id="create_inventory_categorized_month_last_day_recent_two_years_table",
        postgres_conn_id=connection_id,
        sql=create_inventory_categorized_month_last_day_recent_two_years_table_sql,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
        }
    )

    normalize_inventory_csv = PythonOperator(
        task_id='normalize_inventory_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/inventory_{{ ds }}.csv",
            'target': "/data/stg/inventory_{{ ds }}.csv"
        }
    )

    load_products_to_stg_products_table = PythonOperator(
        task_id='load_products_to_stg_products_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/products_{{ ds }}.csv",
            'table_name': 'stg_products'
        },
    )

    load_inventory_to_stg_inventory_table = PythonOperator(
        task_id='load_inventory_to_stg_inventory_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/inventory_{{ ds }}.csv",
            'table_name': 'stg_inventory'
        },
    )

    compute_inventory_nums_categorized_recent_month = PostgresOperator(
        task_id="compute_inventory_nums_categorized_recent_month",
        postgres_conn_id=connection_id,
        sql=compute_inventory_nums_categorized_recent_month_sql,
    )

    compute_inventory_categorized_month_last_day_recent_two_years = PostgresOperator(
        task_id="compute_inventory_categorized_month_last_day_recent_two_years",
        postgres_conn_id=connection_id,
        sql=compute_inventory_categorized_month_last_day_recent_two_years_sql,
    )

    check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table
    check_stg_inventory_csv_readiness >> normalize_inventory_csv >> create_stg_inventory_table >> load_inventory_to_stg_inventory_table

    load_products_to_stg_products_table >> create_inventory_nums_categorized_recent_month_table
    load_inventory_to_stg_inventory_table >> create_inventory_nums_categorized_recent_month_table

    create_inventory_nums_categorized_recent_month_table >> compute_inventory_nums_categorized_recent_month

    load_products_to_stg_products_table >> create_inventory_categorized_month_last_day_recent_two_years_table
    load_inventory_to_stg_inventory_table >> create_inventory_categorized_month_last_day_recent_two_years_table

    create_inventory_categorized_month_last_day_recent_two_years_table >> compute_inventory_categorized_month_last_day_recent_two_years