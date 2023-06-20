from __future__ import annotations

import functools
from datetime import datetime
from unittest import mock
from unittest.mock import MagicMock, call

from airflow.models import DAG
from reusable_job_cluster.operators import (
    DatabricksReusableJobCluster, XCOM_PARENT_RUN_ID_KEY, XCOM_INFINITE_LOOP_CLUSTER_ID
)
from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook

DATE = "2017-04-20"
TASK_ID = "databricks-operator"
DEFAULT_CONN_ID = "databricks_default"
NOTEBOOK_TASK = {"notebook_path": "/test"}
TEMPLATED_NOTEBOOK_TASK = {"notebook_path": "/test-{{ ds }}"}
RENDERED_TEMPLATED_NOTEBOOK_TASK = {"notebook_path": f"/test-{DATE}"}
SPARK_JAR_TASK = {"main_class_name": "com.databricks.Test"}
SPARK_PYTHON_TASK = {"python_file": "test.py", "parameters": ["--param", "123"]}
SPARK_SUBMIT_TASK = {
    "parameters": ["--class", "org.apache.spark.examples.SparkPi", "dbfs:/path/to/examples.jar", "10"]
}
NEW_CLUSTER = {
    "spark_version": "2.0.x-scala2.10",
    "node_type_id": "development-node",
    "num_workers": 1,
    "enable_elastic_disk": True,
}
EXISTING_CLUSTER_ID = "existing-cluster-id"
RUN_NAME = "run-name"
RUN_ID = 1
RUN_PAGE_URL = "run-page-url"
JOB_ID = "42"
JOB_NAME = "job-name"
NOTEBOOK_PARAMS = {"dry-run": "true", "oldest-time-to-consider": "1457570074236"}
JAR_PARAMS = ["param1", "param2"]
RENDERED_TEMPLATED_JAR_PARAMS = [f"/test-{DATE}"]
TEMPLATED_JAR_PARAMS = ["/test-{{ ds }}"]
PYTHON_PARAMS = ["john doe", "35"]
SPARK_SUBMIT_PARAMS = ["--class", "org.apache.spark.examples.SparkPi"]
DBT_TASK = {
    "commands": ["dbt deps", "dbt seed", "dbt run"],
    "schema": "jaffle_shop",
    "warehouse_id": "123456789abcdef0",
}


def mock_dict(d: dict):
    m = MagicMock()
    m.return_value = d
    return m


def make_run_with_state_mock(
        lifecycle_state: str, result_state: str, state_message: str = "", run_id=1, job_id=JOB_ID
):
    return mock_dict(
        {
            "job_id": job_id,
            "run_id": run_id,
            "state": {
                "life_cycle_state": lifecycle_state,
                "result_state": result_state,
                "state_message": state_message,
            },
        }
    )

NOTEBOOK_DIR_PATH = "somepath"
NOTEBOOK_NAME = "parent_notebook_name"

@functools.lru_cache(maxsize=1)
def get_default_ops():
    dag = DAG(dag_id="some_dag", start_date=datetime(2017, 1, 1))

    return (DatabricksReusableJobCluster.builder()
                                           .with_new_cluster(NEW_CLUSTER)
                                           .with_dag(dag)
                                           .with_timeout_seconds(6000)
                                           .with_task_prefix(task_prefix="reusable_cluster")
                                           .with_run_now_mode()
                                           .with_parent_notebook_dir_path(NOTEBOOK_DIR_PATH)
                                           .with_parent_notebook_name(NOTEBOOK_NAME)
                                           .with_tags({"tag1": "value1"})
                                           .build_operators()
                                           )

class TestDatabricksReusableJobCluster:

    def test_builder_values(self):
        """
        Test the initializer with the named parameters.
        """
        create, delete, existing_cluster_id = get_default_ops()

        assert create.task_id == "reusable_cluster_create"
        assert delete.task_id == "reusable_cluster_destroy"
        assert existing_cluster_id == "{{ task_instance.xcom_pull(task_ids='reusable_cluster_create', " \
                                      "key='infinite_loop_cluster_id') }}"
        assert create._get_notebook_base64() is not None, "Notebook should be set"

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_get_dir(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        db_api_mock = db_hook_mock.return_value
        email = "example@domain.com"
        db_api_mock.get_current_user.return_value = {"userName": email}

        assert create._get_standard_root_dir() == f"/Users/{email}/.reusable_job_cluster/notebooks"
        db_api_mock.get_current_user.assert_called_once()

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_get_cluster_id(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        run_id = 123
        cluster_id = "abc123"
        db_api_mock = db_hook_mock.return_value
        db_api_mock.get_run.return_value = {"tasks": [{
            "cluster_instance": {
                "cluster_id": cluster_id
            }
        }]}

        assert create._get_cluster_id(run_id) == cluster_id
        db_api_mock.get_run.assert_called_once_with(run_id)

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_in_submit_run_mode(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        input_json = {
            "some": "json"
        }
        run_id = 123
        cluster_id = "abc123"
        db_api_mock = db_hook_mock.return_value
        db_api_mock.submit_run.return_value = run_id
        db_api_mock.get_run.return_value = {"tasks": [{
            "cluster_instance": {
                "cluster_id": cluster_id
            }
        }]}

        create.execute_in_submit_run_mode(input_json)
        assert create.run_id == run_id
        assert create.cluster_id == cluster_id

        db_api_mock.submit_run.assert_called_once_with(input_json)
        db_api_mock.submit_run.get_run(run_id)

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_in_run_now_mode_job_not_found(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        db_api_mock = db_hook_mock.return_value
        input_json = {
            "some": "json"
        }
        job_id = 678
        run_id = 123
        cluster_id = "abc123"
        db_api_mock.find_job_id_by_name.return_value = None
        db_api_mock.create_job.return_value = job_id
        db_api_mock.run_now.return_value = run_id
        db_api_mock.get_run.return_value = {"tasks": [{
            "cluster_instance": {
                "cluster_id": cluster_id
            }
        }]}

        create.execute_in_run_now_mode(input_json)
        assert create.run_id == run_id
        assert create.cluster_id == cluster_id

        # default job name is None
        db_api_mock.find_job_id_by_name.assert_called_once_with(create.job_name)
        db_api_mock.create_job.assert_called_once_with(input_json)
        db_api_mock.run_now.assert_called_once_with({"job_id": job_id})
        db_api_mock.get_run.assert_called_once_with(run_id)

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_in_run_now_mode_job_found(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        db_api_mock = db_hook_mock.return_value
        input_json = {
            "some": "json"
        }
        job_id = 678
        run_id = 123
        cluster_id = "abc123"
        db_api_mock.find_job_id_by_name.return_value = job_id
        db_api_mock.reset_job.return_value = None
        db_api_mock.run_now.return_value = run_id
        db_api_mock.get_run.return_value = {"tasks": [{
            "cluster_instance": {
                "cluster_id": cluster_id
            }
        }]}

        create.execute_in_run_now_mode(input_json)
        assert create.run_id == run_id
        assert create.cluster_id == cluster_id

        # default job name is None
        db_api_mock.find_job_id_by_name.assert_called_once_with(create.job_name)
        db_api_mock.reset_job.assert_called_once_with(job_id, input_json)
        db_api_mock.run_now.assert_called_once_with({"job_id": job_id})
        db_api_mock.get_run.assert_called_once_with(run_id)

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_make_notebook_if_exists(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        expected_notebook_path = f"{NOTEBOOK_DIR_PATH}/{NOTEBOOK_NAME}"
        db_api_mock = db_hook_mock.return_value
        db_api_mock.do_mkdirs.return_value = None
        db_api_mock.do_export_notebook.return_value = create._get_notebook_base64()
        assert create._make_notebook_if_not_exists() == expected_notebook_path
        db_api_mock.do_mkdirs.assert_called_once_with(NOTEBOOK_DIR_PATH)
        db_api_mock.do_export_notebook.assert_called_once_with(expected_notebook_path)

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_make_notebook_if_not_exists(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        expected_notebook_path = f"{NOTEBOOK_DIR_PATH}/{NOTEBOOK_NAME}"
        db_api_mock = db_hook_mock.return_value
        db_api_mock.do_mkdirs.return_value = None
        db_api_mock.do_export_notebook.side_effect = Exception("some error")
        db_api_mock.do_import_notebook.return_value = None
        assert create._make_notebook_if_not_exists() == expected_notebook_path
        db_api_mock.do_mkdirs.assert_called_once_with(NOTEBOOK_DIR_PATH)
        db_api_mock.do_export_notebook.assert_called_once_with(expected_notebook_path)
        db_api_mock.do_import_notebook.assert_called_once_with(expected_notebook_path, create._get_notebook_base64())

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_make_notebook_if_content_diff(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        expected_notebook_path = f"{NOTEBOOK_DIR_PATH}/{NOTEBOOK_NAME}"
        db_api_mock = db_hook_mock.return_value
        db_api_mock.do_mkdirs.return_value = None
        db_api_mock.do_export_notebook.return_value = "some other content"
        db_api_mock.do_import_notebook.return_value = None
        assert create._make_notebook_if_not_exists() == expected_notebook_path
        db_api_mock.do_mkdirs.assert_called_once_with(NOTEBOOK_DIR_PATH)
        db_api_mock.do_export_notebook.assert_called_once_with(expected_notebook_path)
        db_api_mock.do_import_notebook.assert_called_once_with(expected_notebook_path, create._get_notebook_base64())

    @mock.patch("reusable_job_cluster.operators.DatabricksCreateReusableJobClusterOperator._get_hook")
    def test_builder_execute_create(self, db_hook_mock):
        create, delete, existing_cluster_id = get_default_ops()
        db_api_mock = db_hook_mock.return_value
        db_api_mock.do_mkdirs.return_value = None
        db_api_mock.do_export_notebook.return_value = create._get_notebook_base64()
        job_id = 678
        run_id = 123
        cluster_id = "abc123"
        db_api_mock.find_job_id_by_name.return_value = job_id
        db_api_mock.reset_job.return_value = None
        db_api_mock.run_now.return_value = run_id
        db_api_mock.get_run.return_value = {"tasks": [{
            "cluster_instance": {
                "cluster_id": cluster_id
            }
        }]}
        email = "example@domain.com"
        # mock get user for tags
        db_api_mock.get_current_user.return_value = {"userName": email}
        ti_mock = mock.MagicMock()
        ti_mock.xcom_push.return_value = None
        create.execute({"ti": ti_mock})
        ti_mock.assert_has_calls([
            call.xcom_push(key=XCOM_PARENT_RUN_ID_KEY, value=run_id),
            call.xcom_push(key=XCOM_INFINITE_LOOP_CLUSTER_ID, value=cluster_id),
        ])
