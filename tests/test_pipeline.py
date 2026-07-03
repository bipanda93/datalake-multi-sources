import pytest
import sys
import os

sys.path.insert(0, '/opt/airflow/dags')


def test_dag1_file_exists():
    """Le fichier DAG 1 doit exister"""
    assert os.path.exists('/opt/airflow/dags/dag1_ingestion_bronze.py')


def test_dag2_file_exists():
    """Le fichier DAG 2 doit exister"""
    assert os.path.exists('/opt/airflow/dags/dag2_quality_check.py')


def test_dag3_file_exists():
    """Le fichier DAG 3 doit exister"""
    assert os.path.exists('/opt/airflow/dags/dag3_silver_transform.py')


def test_dag4_file_exists():
    """Le fichier DAG 4 doit exister"""
    assert os.path.exists('/opt/airflow/dags/dag4_gold_restitution.py')


def test_dag1_imports_correctly():
    """DAG 1 doit s'importer sans erreur de syntaxe"""
    import dag1_ingestion_bronze
    assert dag1_ingestion_bronze.dag.dag_id == 'dag1_ingestion_bronze'


def test_dag2_imports_correctly():
    """DAG 2 doit s'importer sans erreur de syntaxe"""
    import dag2_quality_check
    assert dag2_quality_check.dag.dag_id == 'dag2_quality_check'


def test_dag3_imports_correctly():
    """DAG 3 doit s'importer sans erreur de syntaxe"""
    import dag3_silver_transform
    assert dag3_silver_transform.dag.dag_id == 'dag3_silver_transform'


def test_dag4_imports_correctly():
    """DAG 4 doit s'importer sans erreur de syntaxe"""
    import dag4_gold_restitution
    assert dag4_gold_restitution.dag.dag_id == 'dag4_gold_restitution'


def test_dag1_has_5_ingestion_tasks_plus_trigger():
    """DAG 1 doit avoir les 5 sources + trigger vers DAG 2"""
    import dag1_ingestion_bronze
    task_ids = [t.task_id for t in dag1_ingestion_bronze.dag.tasks]
    assert 'extract_load_postgres_bronze' in task_ids
    assert 'extract_load_mongo_bronze' in task_ids
    assert 'extract_load_redis_bronze' in task_ids
    assert 'extract_load_api_bronze' in task_ids
    assert 'extract_load_geolocation_bronze' in task_ids
    assert 'trigger_quality_check' in task_ids


def test_dag4_has_restitution_task():
    """DAG 4 doit avoir la tache de restitution MongoDB"""
    import dag4_gold_restitution
    task_ids = [t.task_id for t in dag4_gold_restitution.dag.tasks]
    assert 'restitution_mongodb' in task_ids