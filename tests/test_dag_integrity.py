import os
import sys

from airflow.models import DagBag

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "airflow"))

def test_dag_integrity():
    dagbag = DagBag(dag_folder="airflow/dags")
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"