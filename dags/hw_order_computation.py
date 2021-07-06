import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.operators.python import PythonOperator
from airflow.sensors.bash import BashSensor
import hw_order_computation_sqls as comps_sqls
import process_orders_sqls as procs_sqls

default_args = {"owner": "airflow"}
connection_id = 'dwh'
default_end_time = '2999-12-31 23:59:59'

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
    dag_id="order_computation",
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
    create_stg_products_table = PostgresOperator(
        task_id="create_stg_products_table",
        postgres_conn_id=connection_id,
        sql=procs_sqls.create_stg_products_sql,
    )

    normalize_products_csv = PythonOperator(
        task_id='normalize_products_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/products_{{ ds }}.csv",
            'target': "/data/stg/products_{{ ds }}.csv"
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

    check_stg_products_csv_readiness >> normalize_products_csv >> create_stg_products_table >> load_products_to_stg_products_table

    # orders
    check_stg_orders_csv_readiness = BashSensor(
        task_id="check_stg_orders_csv_readiness",
        bash_command="""
            ls /data/raw/orders_{{ ds }}.csv
        """,
    )

    normalize_orders_csv = PythonOperator(
        task_id='normalize_orders_csv',
        python_callable=normalize_csv,
        op_kwargs={
            'source': "/data/raw/orders_{{ ds }}.csv",
            'target': "/data/stg/orders_{{ ds }}.csv"
        }
    )

    create_stg_orders_table = PostgresOperator(
        task_id="create_stg_orders_table",
        postgres_conn_id=connection_id,
        sql=procs_sqls.create_stg_orders_sql,
    )

    load_orders_to_stg_orders_table = PythonOperator(
        task_id='load_orders_to_stg_orders_table',
        python_callable=load_csv_to_postgres,
        op_kwargs={
            'csv_filepath': "/data/stg/orders_{{ ds }}.csv",
            'table_name': 'stg_orders'
        },
    )

    create_unfinished_orders_count_table = PostgresOperator(
        task_id="create_unfinished_orders_count_table",
        postgres_conn_id=connection_id,
        sql=comps_sqls.create_unfinished_orders_count_table_sql,
    )

    create_orders_count_recent_two_years_quaterly_table = PostgresOperator(
        task_id="create_orders_count_recent_two_years_quaterly_table",
        postgres_conn_id=connection_id,
        sql=comps_sqls.create_orders_count_recent_two_years_quaterly_table_sql,
    )

    create_orders_count_recent_two_years_quaterly_category_table = PostgresOperator(
        task_id="create_orders_count_recent_two_years_quaterly_category_table",
        postgres_conn_id=connection_id,
        sql=comps_sqls.create_orders_count_recent_two_years_quaterly_category_table_sql,
    )

    create_unfinished_orders_recent_month_count_table = PostgresOperator(
        task_id="create_unfinished_orders_recent_month_count_table",
        postgres_conn_id=connection_id,
        sql=comps_sqls.create_unfinished_orders_recent_month_count_table_sql,
    )

    create_unfinished_orders_recent_two_years_monthly_count_table = PostgresOperator(
        task_id="create_unfinished_orders_recent_two_years_monthly_count_table",
        postgres_conn_id=connection_id,
        sql=comps_sqls.create_unfinished_orders_recent_two_years_monthly_count_table_sql,
    )

    # computation

    compute_unfinished_orders_count = PostgresOperator(
        task_id="compute_unfinished_orders_count",
        postgres_conn_id=connection_id,
        sql=comps_sqls.unfinished_orders_count_sql,
    )

    compute_orders_count_recent_two_years_quaterly = PostgresOperator(
        task_id="compute_orders_count_recent_two_years_quaterly",
        postgres_conn_id=connection_id,
        sql=comps_sqls.orders_count_recent_two_years_quaterly_sql,
    )

    compute_orders_count_recent_two_years_quaterly_category_sql = PostgresOperator(
        task_id="compute_orders_count_recent_two_years_quaterly_category",
        postgres_conn_id=connection_id,
        sql=comps_sqls.orders_count_recent_two_years_quaterly_category_sql,
    )

    compute_unfinished_orders_recent_month_count_sql = PostgresOperator(
        task_id="compute_unfinished_orders_recent_month_count",
        postgres_conn_id=connection_id,
        sql=comps_sqls.unfinished_orders_recent_month_count_sql,
    )

    compute_unfinished_orders_recent_two_years_monthly_count_sql = PostgresOperator(
        task_id="compute_unfinished_orders_recent_two_years_monthly_count",
        postgres_conn_id=connection_id,
        sql=comps_sqls.unfinished_orders_recent_two_years_monthly_count_sql,
    )

    check_stg_orders_csv_readiness >> normalize_orders_csv >> create_stg_orders_table >> load_orders_to_stg_orders_table

    load_orders_to_stg_orders_table >> create_unfinished_orders_count_table >> compute_unfinished_orders_count

    load_orders_to_stg_orders_table >> create_orders_count_recent_two_years_quaterly_table >> compute_orders_count_recent_two_years_quaterly

    load_orders_to_stg_orders_table >> create_orders_count_recent_two_years_quaterly_category_table >> compute_orders_count_recent_two_years_quaterly_category_sql

    load_orders_to_stg_orders_table >> create_unfinished_orders_recent_month_count_table >> compute_unfinished_orders_recent_month_count_sql

    load_orders_to_stg_orders_table >> create_unfinished_orders_recent_two_years_monthly_count_table >> compute_unfinished_orders_recent_two_years_monthly_count_sql

