from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from io import BytesIO
import json
import pandas as pd

default_args = {
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id='dag2_quality_check',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='DAG 2 - Controle qualite Great Expectations sur les donnees Bronze'
)

BUCKET_BRONZE = 'datalake-bronze'


def get_minio_client():
    from minio import Minio
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )


def read_csv_from_bronze(client, object_name):
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_csv(BytesIO(data))


def read_json_from_bronze(client, object_name):
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.DataFrame(json.loads(data))


def run_expectation(df, column, expectation_type):
    """Execute une expectation Great Expectations (mode ephemere) sur un DataFrame."""
    import great_expectations as gx

    context = gx.get_context(mode='ephemeral')
    data_source = context.data_sources.add_pandas('pandas_datasource')
    data_asset = data_source.add_dataframe_asset(name='asset')
    batch_definition = data_asset.add_batch_definition_whole_dataframe('batch_def')
    batch = batch_definition.get_batch(batch_parameters={'dataframe': df})

    if expectation_type == 'not_null':
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
    elif expectation_type == 'unique':
        expectation = gx.expectations.ExpectColumnValuesToBeUnique(column=column)
    elif expectation_type == 'positive':
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=column, min_value=0.01
        )
    elif expectation_type == 'non_negative':
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=column, min_value=0
        )
    else:
        raise ValueError(f"Type d'expectation inconnu : {expectation_type}")

    return batch.validate(expectation)


# ============================================================
# VALIDATION - orders : order_id non null ET unique
# ============================================================
def validate_orders(**context):
    client = get_minio_client()
    dt = context['ds']
    df = read_csv_from_bronze(client, f'postgres/orders/dt={dt}/orders.csv')

    r1 = run_expectation(df, 'order_id', 'not_null')
    r2 = run_expectation(df, 'order_id', 'unique')

    print(f"orders.order_id not_null : {r1.success}")
    print(f"orders.order_id unique   : {r2.success}")

    if not (r1.success and r2.success):
        raise ValueError("Validation ECHOUEE : orders.order_id doit etre non-null et unique")

    print(f"orders : {len(df)} lignes VALIDEES")


task_validate_orders = PythonOperator(
    task_id='validate_orders',
    python_callable=validate_orders,
    provide_context=True,
    dag=dag
)


# ============================================================
# VALIDATION - customers : customer_id non null ET unique
# ============================================================
def validate_customers(**context):
    client = get_minio_client()
    dt = context['ds']
    df = read_csv_from_bronze(client, f'postgres/customers/dt={dt}/customers.csv')

    r1 = run_expectation(df, 'customer_id', 'not_null')
    r2 = run_expectation(df, 'customer_id', 'unique')

    print(f"customers.customer_id not_null : {r1.success}")
    print(f"customers.customer_id unique   : {r2.success}")

    if not (r1.success and r2.success):
        raise ValueError("Validation ECHOUEE : customers.customer_id doit etre non-null et unique")

    print(f"customers : {len(df)} lignes VALIDEES")


task_validate_customers = PythonOperator(
    task_id='validate_customers',
    python_callable=validate_customers,
    provide_context=True,
    dag=dag
)


# ============================================================
# VALIDATION - order_items : price > 0
# ============================================================
def validate_order_items(**context):
    client = get_minio_client()
    dt = context['ds']
    df = read_csv_from_bronze(client, f'postgres/order_items/dt={dt}/order_items.csv')
    df['price'] = pd.to_numeric(df['price'], errors='coerce')

    r1 = run_expectation(df, 'price', 'positive')

    print(f"order_items.price > 0 : {r1.success}")

    if not r1.success:
        raise ValueError("Validation ECHOUEE : order_items.price doit etre strictement positif")

    print(f"order_items : {len(df)} lignes VALIDEES")


task_validate_order_items = PythonOperator(
    task_id='validate_order_items',
    python_callable=validate_order_items,
    provide_context=True,
    dag=dag
)


# ============================================================
# VALIDATION - products (MongoDB) : product_id non null
# ============================================================
def validate_products(**context):
    client = get_minio_client()
    dt = context['ds']
    df = read_json_from_bronze(client, f'mongo/products/dt={dt}/products.json')

    r1 = run_expectation(df, 'product_id', 'not_null')

    print(f"products.product_id not_null : {r1.success}")

    if not r1.success:
        raise ValueError("Validation ECHOUEE : products.product_id ne doit jamais etre null")

    print(f"products : {len(df)} documents VALIDES")


task_validate_products = PythonOperator(
    task_id='validate_products',
    python_callable=validate_products,
    provide_context=True,
    dag=dag
)


# ============================================================
# VALIDATION - payments (Redis) : payment_value >= 0
# ============================================================
def validate_payments(**context):
    client = get_minio_client()
    dt = context['ds']
    df = read_json_from_bronze(client, f'redis/payments/dt={dt}/payments_snapshot.json')
    df['payment_value'] = pd.to_numeric(df['payment_value'], errors='coerce')

    r1 = run_expectation(df, 'payment_value', 'non_negative')

    print(f"payments.payment_value >= 0 : {r1.success}")

    if not r1.success:
        raise ValueError("Validation ECHOUEE : payments.payment_value ne doit jamais etre negatif")

    print(f"payments : {len(df)} entrees VALIDEES")


task_validate_payments = PythonOperator(
    task_id='validate_payments',
    python_callable=validate_payments,
    provide_context=True,
    dag=dag
)


# ============================================================
# SYNTHESE - ne s'execute QUE si toutes les validations passent
# (comportement natif Airflow : trigger_rule=ALL_SUCCESS par defaut)
# ============================================================
def quality_check_passed(**context):
    print("=" * 60)
    print("TOUTES LES VALIDATIONS GREAT EXPECTATIONS SONT PASSEES")
    print("Le pipeline peut continuer vers la zone Silver (DAG 3)")
    print("=" * 60)


task_quality_passed = PythonOperator(
    task_id='quality_check_passed',
    python_callable=quality_check_passed,
    dag=dag
)


# ============================================================
# DEPENDANCES
# ============================================================
[
    task_validate_orders,
    task_validate_customers,
    task_validate_order_items,
    task_validate_products,
    task_validate_payments,
] >> task_quality_passed

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

task_trigger_dag3 = TriggerDagRunOperator(
    task_id='trigger_silver_transform',
    trigger_dag_id='dag3_silver_transform',
    wait_for_completion=False,
    dag=dag
)

task_quality_passed >> task_trigger_dag3