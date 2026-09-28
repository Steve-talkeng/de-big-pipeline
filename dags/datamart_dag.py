from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'datamart_update',
    default_args=default_args,
    description='Update datamart',
    schedule='@daily',
    catchup=False,
)


export_csv = BashOperator(
    task_id='export_csv',
    bash_command='docker exec de-big-postgres-src psql -U postgres -d mydb -c "\\COPY orders TO \'/tmp/orders.csv\' WITH CSV HEADER;"',
    dag=dag,
)


copy_to_minio = BashOperator(
    task_id='copy_to_minio',
    bash_command="""
    docker cp de-big-postgres-src:/tmp/orders.csv /tmp/orders.csv &&
    docker cp /tmp/orders.csv de-big-minio:/tmp/orders.csv &&
    docker exec de-big-minio mc cp /tmp/orders.csv myminio/data/orders.csv
    """,
    dag=dag,
)


truncate_clickhouse = BashOperator(
    task_id='truncate_clickhouse',
    bash_command='docker exec de-big-clickhouse clickhouse-client --query "TRUNCATE TABLE default.orders;"',
    dag=dag,
)


load_to_clickhouse = BashOperator(
    task_id='load_to_clickhouse',
    bash_command='docker exec de-big-minio mc cat myminio/data/orders.csv | docker exec -i de-big-clickhouse clickhouse-client --query "INSERT INTO default.orders FORMAT CSVWithNames"',
    dag=dag,
)


check_data = BashOperator(
    task_id='check_data',
    bash_command='docker exec de-big-clickhouse clickhouse-client --query "SELECT COUNT(*) FROM default.orders;"',
    dag=dag,
)


check_view = BashOperator(
    task_id='check_view',
    bash_command='docker exec de-big-clickhouse clickhouse-client --query "SELECT * FROM datamart.sales_by_product;"',
    dag=dag,
)


notify_success = EmptyOperator(
    task_id='notify_success',
    dag=dag,
)


export_csv >> copy_to_minio >> truncate_clickhouse >> load_to_clickhouse >> check_data >> check_view >> notify_success
