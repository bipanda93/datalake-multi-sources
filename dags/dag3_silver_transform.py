from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from io import BytesIO
import json
import pandas as pd

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id='dag3_silver_transform',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='DAG 3 - Nettoyage et normalisation Bronze -> Silver (format Parquet)'
)

BUCKET_BRONZE = 'datalake-bronze'
BUCKET_SILVER = 'datalake-silver'


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


def write_parquet_to_silver(client, df, object_name):
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    data = buffer.read()
    client.put_object(
        BUCKET_SILVER, object_name,
        BytesIO(data), length=len(data)
    )
    return len(data)


# ============================================================
# TRANSFORM - orders : typage des 5 colonnes de dates
# ============================================================
def transform_orders_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_csv_from_bronze(client, f'postgres/orders/dt={dt}/orders.csv')

    date_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df = df.drop_duplicates(subset=['order_id'])

    size = write_parquet_to_silver(client, df, f'orders/dt={dt}/orders.parquet')
    print(f"Silver : orders ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_orders = PythonOperator(
    task_id='transform_orders_silver',
    python_callable=transform_orders_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - customers : nettoyage textuel + dedup
# ============================================================
def transform_customers_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_csv_from_bronze(client, f'postgres/customers/dt={dt}/customers.csv')

    df['customer_city'] = df['customer_city'].str.strip().str.lower()
    df['customer_state'] = df['customer_state'].str.strip().str.upper()
    df = df.drop_duplicates(subset=['customer_id'])

    size = write_parquet_to_silver(client, df, f'customers/dt={dt}/customers.parquet')
    print(f"Silver : customers ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_customers = PythonOperator(
    task_id='transform_customers_silver',
    python_callable=transform_customers_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - order_items : typage price/freight_value
# ============================================================
def transform_order_items_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_csv_from_bronze(client, f'postgres/order_items/dt={dt}/order_items.csv')

    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['freight_value'] = pd.to_numeric(df['freight_value'], errors='coerce')
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    df = df.dropna(subset=['price'])

    size = write_parquet_to_silver(client, df, f'order_items/dt={dt}/order_items.parquet')
    print(f"Silver : order_items ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_order_items = PythonOperator(
    task_id='transform_order_items_silver',
    python_callable=transform_order_items_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - products : suppression nulls sur product_id
# ============================================================
def transform_products_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_json_from_bronze(client, f'mongo/products/dt={dt}/products.json')

    df = df.dropna(subset=['product_id'])
    df = df.drop_duplicates(subset=['product_id'])
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    size = write_parquet_to_silver(client, df, f'products/dt={dt}/products.parquet')
    print(f"Silver : products ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_products = PythonOperator(
    task_id='transform_products_silver',
    python_callable=transform_products_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - reviews : typage review_score
# ============================================================
def transform_reviews_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_json_from_bronze(client, f'mongo/reviews/dt={dt}/reviews.json')

    df['review_score'] = pd.to_numeric(df['review_score'], errors='coerce')
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])
    df = df.dropna(subset=['review_score'])

    size = write_parquet_to_silver(client, df, f'reviews/dt={dt}/reviews.parquet')
    print(f"Silver : reviews ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_reviews = PythonOperator(
    task_id='transform_reviews_silver',
    python_callable=transform_reviews_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - payments : typage payment_value
# ============================================================
def transform_payments_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_json_from_bronze(client, f'redis/payments/dt={dt}/payments_snapshot.json')

    df['payment_value'] = pd.to_numeric(df['payment_value'], errors='coerce')
    df['payment_installments'] = pd.to_numeric(df['payment_installments'], errors='coerce')
    df = df.dropna(subset=['payment_value'])

    size = write_parquet_to_silver(client, df, f'payments/dt={dt}/payments.parquet')
    print(f"Silver : payments ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_payments = PythonOperator(
    task_id='transform_payments_silver',
    python_callable=transform_payments_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - sellers : dedup
# ============================================================
def transform_sellers_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_json_from_bronze(client, f'api/sellers/dt={dt}/sellers.json')

    df = df.drop_duplicates(subset=['seller_id'])
    df['seller_city'] = df['seller_city'].str.strip().str.lower()
    df['seller_state'] = df['seller_state'].str.strip().str.upper()

    size = write_parquet_to_silver(client, df, f'sellers/dt={dt}/sellers.parquet')
    print(f"Silver : sellers ({len(df)} lignes, {size/1000:.1f} Ko Parquet)")


task_transform_sellers = PythonOperator(
    task_id='transform_sellers_silver',
    python_callable=transform_sellers_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRANSFORM - geolocation : DEDOUBLONNAGE MASSIF
# 1M lignes brutes -> une ligne moyenne par code postal
# ============================================================
def transform_geolocation_silver(**context):
    client = get_minio_client()
    dt = context['ds']

    df = read_csv_from_bronze(client, f'csv_batch/geolocation/dt={dt}/geolocation.csv')
    nb_avant = len(df)

    df_grouped = df.groupby('geolocation_zip_code_prefix').agg({
        'geolocation_lat': 'mean',
        'geolocation_lng': 'mean',
        'geolocation_city': 'first',
        'geolocation_state': 'first',
    }).reset_index()

    size = write_parquet_to_silver(client, df_grouped, f'geolocation/dt={dt}/geolocation.parquet')
    print(f"Silver : geolocation ({nb_avant} lignes brutes -> {len(df_grouped)} codes postaux uniques, {size/1000:.1f} Ko Parquet)")


task_transform_geolocation = PythonOperator(
    task_id='transform_geolocation_silver',
    python_callable=transform_geolocation_silver,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRIGGER (PUSH) - Declenche automatiquement DAG 4 (Gold)
# ============================================================
task_trigger_dag4 = TriggerDagRunOperator(
    task_id='trigger_gold_aggregation',
    trigger_dag_id='dag4_gold_restitution',
    wait_for_completion=False,
    dag=dag
)


# ============================================================
# DEPENDANCES
# ============================================================
[
    task_transform_orders,
    task_transform_customers,
    task_transform_order_items,
    task_transform_products,
    task_transform_reviews,
    task_transform_payments,
    task_transform_sellers,
    task_transform_geolocation,
] >> task_trigger_dag4