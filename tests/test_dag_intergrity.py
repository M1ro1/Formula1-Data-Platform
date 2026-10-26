from airflow.models import DagBag


def test_dag_integrity():
    dagbag = DagBag(dag_folder="airflow/dags", include_examples=False)
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"