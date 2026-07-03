from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd

default_args = {
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
}

dag = DAG(
    dag_id='dag4_gold_restitution',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='DAG 4 - Agregation KPI Silver -> Gold (MinIO analytique + MongoDB restitution)'
)

BUCKET_SILVER = 'datalake-silver'
BUCKET_GOLD = 'datalake-gold'


def get_minio_client():
    from minio import Minio
    return Minio(
        'minio:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )


def read_parquet_from_silver(client, object_name):
    response = client.get_object(BUCKET_SILVER, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return pd.read_parquet(BytesIO(data), engine='pyarrow')


def write_parquet_to_gold(client, df, object_name):
    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)
    data = buffer.read()
    client.put_object(
        BUCKET_GOLD, object_name,
        BytesIO(data), length=len(data)
    )
    return len(data)


# ============================================================
# AGGREGATE - KPI globaux : CA, commandes, clients, panier moyen
# ============================================================
def aggregate_global_kpi(**context):
    client = get_minio_client()
    dt = context['ds']

    orders = read_parquet_from_silver(client, f'orders/dt={dt}/orders.parquet')
    order_items = read_parquet_from_silver(client, f'order_items/dt={dt}/order_items.parquet')
    customers = read_parquet_from_silver(client, f'customers/dt={dt}/customers.parquet')

    nb_commandes = orders['order_id'].nunique()
    nb_clients = customers['customer_id'].nunique()
    chiffre_affaires = round(order_items['price'].sum(), 2)
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes > 0 else 0

    kpi = pd.DataFrame([{
        'nb_commandes': nb_commandes,
        'nb_clients': nb_clients,
        'chiffre_affaires': chiffre_affaires,
        'panier_moyen': panier_moyen,
    }])

    write_parquet_to_gold(client, kpi, f'global_kpi/dt={dt}/global_kpi.parquet')
    print(f"Gold : KPI globaux -> commandes={nb_commandes}, clients={nb_clients}, CA={chiffre_affaires} BRL, panier moyen={panier_moyen} BRL")

    return kpi.to_dict('records')[0]


task_aggregate_global = PythonOperator(
    task_id='aggregate_global_kpi',
    python_callable=aggregate_global_kpi,
    provide_context=True,
    dag=dag
)


# ============================================================
# AGGREGATE - CA par etat (region_metrics)
# ============================================================
def aggregate_region_kpi(**context):
    client = get_minio_client()
    dt = context['ds']

    orders = read_parquet_from_silver(client, f'orders/dt={dt}/orders.parquet')
    order_items = read_parquet_from_silver(client, f'order_items/dt={dt}/order_items.parquet')
    customers = read_parquet_from_silver(client, f'customers/dt={dt}/customers.parquet')

    merged = order_items.merge(orders[['order_id', 'customer_id']], on='order_id')
    merged = merged.merge(customers[['customer_id', 'customer_state']], on='customer_id')

    region_kpi = merged.groupby('customer_state')['price'].sum().round(2).reset_index()
    region_kpi.columns = ['state', 'chiffre_affaires']
    region_kpi = region_kpi.sort_values('chiffre_affaires', ascending=False)

    write_parquet_to_gold(client, region_kpi, f'region_kpi/dt={dt}/region_kpi.parquet')
    print(f"Gold : KPI regions -> {len(region_kpi)} etats, top etat={region_kpi.iloc[0]['state']}")

    return region_kpi.to_dict('records')


task_aggregate_region = PythonOperator(
    task_id='aggregate_region_kpi',
    python_callable=aggregate_region_kpi,
    provide_context=True,
    dag=dag
)


# ============================================================
# AGGREGATE - Top 10 produits par revenu
# ============================================================
def aggregate_top_products(**context):
    client = get_minio_client()
    dt = context['ds']

    order_items = read_parquet_from_silver(client, f'order_items/dt={dt}/order_items.parquet')

    top_products = order_items.groupby('product_id')['price'].sum().round(2).reset_index()
    top_products.columns = ['product_id', 'revenue']
    top_products = top_products.sort_values('revenue', ascending=False).head(10)

    write_parquet_to_gold(client, top_products, f'top_products/dt={dt}/top_products.parquet')
    print(f"Gold : Top 10 produits -> revenu max={top_products.iloc[0]['revenue']} BRL")

    return top_products.to_dict('records')


task_aggregate_top_products = PythonOperator(
    task_id='aggregate_top_products',
    python_callable=aggregate_top_products,
    provide_context=True,
    dag=dag
)


# ============================================================
# AGGREGATE - Satisfaction client (note moyenne des avis)
# ============================================================
def aggregate_satisfaction(**context):
    client = get_minio_client()
    dt = context['ds']

    reviews = read_parquet_from_silver(client, f'reviews/dt={dt}/reviews.parquet')

    note_moyenne = round(reviews['review_score'].mean(), 2)
    nb_avis = len(reviews)
    repartition = reviews['review_score'].value_counts().sort_index().to_dict()

    satisfaction = pd.DataFrame([{
        'note_moyenne': note_moyenne,
        'nb_avis': nb_avis,
    }])

    write_parquet_to_gold(client, satisfaction, f'satisfaction/dt={dt}/satisfaction.parquet')
    print(f"Gold : Satisfaction -> note moyenne={note_moyenne}/5 sur {nb_avis} avis, repartition={repartition}")

    return {'note_moyenne': note_moyenne, 'nb_avis': nb_avis}


task_aggregate_satisfaction = PythonOperator(
    task_id='aggregate_satisfaction',
    python_callable=aggregate_satisfaction,
    provide_context=True,
    dag=dag
)


# ============================================================
# AGGREGATE - Delai de livraison moyen
# ============================================================
def aggregate_delivery_delay(**context):
    client = get_minio_client()
    dt = context['ds']

    orders = read_parquet_from_silver(client, f'orders/dt={dt}/orders.parquet')

    delivered = orders.dropna(subset=['order_purchase_timestamp', 'order_delivered_customer_date'])
    delivered = delivered.copy()
    delivered['delai_jours'] = (
        delivered['order_delivered_customer_date'] - delivered['order_purchase_timestamp']
    ).dt.days

    delai_moyen = round(delivered['delai_jours'].mean(), 1)

    delay_kpi = pd.DataFrame([{
        'delai_moyen_jours': delai_moyen,
        'nb_commandes_livrees': len(delivered),
    }])

    write_parquet_to_gold(client, delay_kpi, f'delivery_delay/dt={dt}/delivery_delay.parquet')
    print(f"Gold : Delai livraison moyen -> {delai_moyen} jours sur {len(delivered)} commandes livrees")

    return {'delai_moyen_jours': delai_moyen, 'nb_commandes_livrees': len(delivered)}


task_aggregate_delay = PythonOperator(
    task_id='aggregate_delivery_delay',
    python_callable=aggregate_delivery_delay,
    provide_context=True,
    dag=dag
)


# ============================================================
# AGGREGATE - Top vendeurs par CA
# ============================================================
def aggregate_top_sellers(**context):
    client = get_minio_client()
    dt = context['ds']

    order_items = read_parquet_from_silver(client, f'order_items/dt={dt}/order_items.parquet')
    sellers = read_parquet_from_silver(client, f'sellers/dt={dt}/sellers.parquet')

    top_sellers = order_items.groupby('seller_id')['price'].sum().round(2).reset_index()
    top_sellers.columns = ['seller_id', 'chiffre_affaires']
    top_sellers = top_sellers.merge(sellers[['seller_id', 'seller_state']], on='seller_id', how='left')
    top_sellers = top_sellers.sort_values('chiffre_affaires', ascending=False).head(10)

    write_parquet_to_gold(client, top_sellers, f'top_sellers/dt={dt}/top_sellers.parquet')
    print(f"Gold : Top 10 vendeurs -> CA max={top_sellers.iloc[0]['chiffre_affaires']} BRL")

    return top_sellers.to_dict('records')


task_aggregate_top_sellers = PythonOperator(
    task_id='aggregate_top_sellers',
    python_callable=aggregate_top_sellers,
    provide_context=True,
    dag=dag
)


# ============================================================
# RESTITUTION - Consolidation finale -> MongoDB Gold operationnel
# ============================================================
def restitution_mongodb(**context):
    import pymongo

    ti = context['ti']
    dt = context['ds']

    global_kpi = ti.xcom_pull(task_ids='aggregate_global_kpi')
    region_kpi = ti.xcom_pull(task_ids='aggregate_region_kpi')
    top_products = ti.xcom_pull(task_ids='aggregate_top_products')
    satisfaction = ti.xcom_pull(task_ids='aggregate_satisfaction')
    delivery = ti.xcom_pull(task_ids='aggregate_delivery_delay')
    top_sellers = ti.xcom_pull(task_ids='aggregate_top_sellers')

    document = {
        'dt': dt,
        'dag_id': 'dag4_gold_restitution',
        'global_kpi': global_kpi,
        'region_kpi': region_kpi,
        'top_products': top_products,
        'satisfaction': satisfaction,
        'delivery': delivery,
        'top_sellers': top_sellers,
    }

    client = pymongo.MongoClient('mongodb://mongodb:27017/')
    db = client['datalake']
    db.gold_kpi.replace_one({'dt': dt}, document, upsert=True)
    client.close()

    print("=" * 60)
    print("RESTITUTION MONGODB GOLD TERMINEE")
    print(f"Document consolide inséré dans datalake.gold_kpi (dt={dt})")
    print("=" * 60)


task_restitution_mongodb = PythonOperator(
    task_id='restitution_mongodb',
    python_callable=restitution_mongodb,
    provide_context=True,
    dag=dag
)


# ============================================================
# DEPENDANCES
# ============================================================
[
    task_aggregate_global,
    task_aggregate_region,
    task_aggregate_top_products,
    task_aggregate_satisfaction,
    task_aggregate_delay,
    task_aggregate_top_sellers,
] >> task_restitution_mongodb