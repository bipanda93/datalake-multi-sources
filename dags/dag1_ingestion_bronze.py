from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from io import BytesIO

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id='dag1_ingestion_bronze',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='DAG 1 - Ingestion ELT multi-sources vers MinIO Bronze (aucune transformation)'
)

BUCKET_BRONZE = 'datalake-bronze'
DATA_DIR = '/opt/airflow/data'


def get_minio_client():
    from minio import Minio
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )


def upload_bytes_to_bronze(client, object_name, data_bytes):
    client.put_object(
        BUCKET_BRONZE, object_name,
        BytesIO(data_bytes), length=len(data_bytes)
    )


# ============================================================
# SEED - PostgreSQL
# ============================================================
def seed_postgres_source(**context):
    import psycopg2

    conn = psycopg2.connect(
        host='postgres_source', dbname='sales',
        user='datalake', password='datalake'
    )
    cur = conn.cursor()

    tables = {
        'orders': 'olist_orders_dataset.csv',
        'customers': 'olist_customers_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
    }

    for table in tables:
        cur.execute(f"TRUNCATE TABLE {table}")

    for table, filename in tables.items():
        with open(f'{DATA_DIR}/{filename}') as f:
            cur.copy_expert(
                f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')",
                f
            )
    conn.commit()
    cur.close()
    conn.close()
    print("Postgres source alimente (simulation systeme operationnel)")


task_seed_postgres = PythonOperator(
    task_id='seed_postgres_source',
    python_callable=seed_postgres_source,
    dag=dag
)


# ============================================================
# EXTRACT + LOAD - PostgreSQL -> Bronze
# ============================================================
def extract_load_postgres_bronze(**context):
    import psycopg2

    conn = psycopg2.connect(
        host='postgres_source', dbname='sales',
        user='datalake', password='datalake'
    )
    cur = conn.cursor()
    client = get_minio_client()
    dt = context['ds']

    for table in ['orders', 'customers', 'order_items']:
        cur.execute(f"SELECT * FROM {table}")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        output = [','.join(colnames)]
        for row in rows:
            output.append(','.join(str(v) if v is not None else '' for v in row))
        data = '\n'.join(output).encode('utf-8')

        object_name = f'postgres/{table}/dt={dt}/{table}.csv'
        upload_bytes_to_bronze(client, object_name, data)
        print(f"EXTRACT+LOAD brut : {table} ({len(rows)} lignes) -> {object_name}")

    cur.close()
    conn.close()


task_extract_load_postgres = PythonOperator(
    task_id='extract_load_postgres_bronze',
    python_callable=extract_load_postgres_bronze,
    provide_context=True,
    dag=dag
)


# ============================================================
# SEED - MongoDB
# ============================================================
def seed_mongo_source(**context):
    import pymongo
    import csv

    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client['datalake']

    db.products.delete_many({})
    db.reviews.delete_many({})

    with open(f'{DATA_DIR}/olist_products_dataset.csv') as f:
        reader = csv.DictReader(f)
        products = list(reader)
        if products:
            db.products.insert_many(products)

    with open(f'{DATA_DIR}/olist_order_reviews_dataset.csv') as f:
        reader = csv.DictReader(f)
        reviews = list(reader)
        if reviews:
            db.reviews.insert_many(reviews)

    client.close()
    print(f"MongoDB alimente : {len(products)} produits, {len(reviews)} avis")


task_seed_mongo = PythonOperator(
    task_id='seed_mongo_source',
    python_callable=seed_mongo_source,
    dag=dag
)


# ============================================================
# EXTRACT + LOAD - MongoDB -> Bronze
# ============================================================
def extract_load_mongo_bronze(**context):
    import pymongo
    import json
    from bson import ObjectId

    class JSONEncoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, ObjectId):
                return str(o)
            return super().default(o)

    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client['datalake']
    minio_client = get_minio_client()
    dt = context['ds']

    for collection_name in ['products', 'reviews']:
        docs = list(db[collection_name].find())
        data = json.dumps(docs, cls=JSONEncoder).encode('utf-8')

        object_name = f'mongo/{collection_name}/dt={dt}/{collection_name}.json'
        upload_bytes_to_bronze(minio_client, object_name, data)
        print(f"EXTRACT+LOAD brut : {collection_name} ({len(docs)} documents) -> {object_name}")

    client.close()


task_extract_load_mongo = PythonOperator(
    task_id='extract_load_mongo_bronze',
    python_callable=extract_load_mongo_bronze,
    provide_context=True,
    dag=dag
)


# ============================================================
# SEED - Redis (paiements temps reel)
# ============================================================
def seed_redis_source(**context):
    import redis
    import csv

    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    r.flushdb()

    count = 0
    with open(f'{DATA_DIR}/olist_order_payments_dataset.csv') as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = f"payment:{row['order_id']}:{row['payment_sequential']}"
            r.hset(key, mapping=row)
            count += 1

    print(f"Redis alimente : {count} paiements")


task_seed_redis = PythonOperator(
    task_id='seed_redis_source',
    python_callable=seed_redis_source,
    dag=dag
)


# ============================================================
# EXTRACT + LOAD - Redis -> Bronze (snapshot JSON)
# ============================================================
def extract_load_redis_bronze(**context):
    import redis
    import json

    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    client = get_minio_client()
    dt = context['ds']

    keys = r.keys('payment:*')
    snapshot = []
    for key in keys:
        snapshot.append(r.hgetall(key))

    data = json.dumps(snapshot).encode('utf-8')
    object_name = f'redis/payments/dt={dt}/payments_snapshot.json'
    upload_bytes_to_bronze(client, object_name, data)
    print(f"EXTRACT+LOAD brut : payments ({len(snapshot)} entrees) -> {object_name}")


task_extract_load_redis = PythonOperator(
    task_id='extract_load_redis_bronze',
    python_callable=extract_load_redis_bronze,
    provide_context=True,
    dag=dag
)


# ============================================================
# EXTRACT + LOAD - API sellers -> Bronze (vrai appel HTTP)
# ============================================================
def extract_load_api_bronze(**context):
    import requests

    response = requests.get('http://api_sellers:5000/api/sellers', timeout=30)
    response.raise_for_status()
    sellers = response.json()

    client = get_minio_client()
    dt = context['ds']

    data = response.content
    object_name = f'api/sellers/dt={dt}/sellers.json'
    upload_bytes_to_bronze(client, object_name, data)
    print(f"EXTRACT+LOAD brut : sellers ({len(sellers)} vendeurs) -> {object_name}")


task_extract_load_api = PythonOperator(
    task_id='extract_load_api_bronze',
    python_callable=extract_load_api_bronze,
    provide_context=True,
    dag=dag
)


# ============================================================
# EXTRACT + LOAD - CSV batch (geolocation) -> Bronze
# ============================================================
def extract_load_geolocation_bronze(**context):
    client = get_minio_client()
    dt = context['ds']

    filepath = f'{DATA_DIR}/olist_geolocation_dataset.csv'
    with open(filepath, 'rb') as f:
        data = f.read()

    object_name = f'csv_batch/geolocation/dt={dt}/geolocation.csv'
    upload_bytes_to_bronze(client, object_name, data)

    with open(filepath) as f:
        nb_lignes = sum(1 for _ in f) - 1

    print(f"EXTRACT+LOAD brut : geolocation ({nb_lignes} lignes, {len(data)/1_000_000:.1f} Mo) -> {object_name}")


task_extract_load_geolocation = PythonOperator(
    task_id='extract_load_geolocation_bronze',
    python_callable=extract_load_geolocation_bronze,
    provide_context=True,
    dag=dag
)


# ============================================================
# TRIGGER (PUSH) - Declenche automatiquement DAG 2 (Quality Check)
# une fois que TOUTES les sources Bronze sont ingerees avec succes.
# wait_for_completion=False : fire-and-forget, DAG 1 se termine
# immediatement sans attendre le resultat de DAG 2.
# ============================================================
task_trigger_dag2 = TriggerDagRunOperator(
    task_id='trigger_quality_check',
    trigger_dag_id='dag2_quality_check',
    wait_for_completion=False,
    dag=dag
)


# ============================================================
# DEPENDANCES
# ============================================================
task_seed_postgres >> task_extract_load_postgres
task_seed_mongo >> task_extract_load_mongo
task_seed_redis >> task_extract_load_redis

[
    task_extract_load_postgres,
    task_extract_load_mongo,
    task_extract_load_redis,
    task_extract_load_api,
    task_extract_load_geolocation,
] >> task_trigger_dag2