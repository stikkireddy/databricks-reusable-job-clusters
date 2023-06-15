from __future__ import annotations

import base64
import time
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Tuple

from airflow import DAG
from airflow.models import BaseOperator
from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook
from reusable_job_cluster.vendor.utils.databricks import normalise_json_content
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from airflow.utils.context import Context

XCOM_PARENT_RUN_ID_KEY = "parent_run_id"
XCOM_INFINITE_LOOP_CLUSTER_ID = "infinite_loop_cluster_id"


class ReusableClusterMode(Enum):
    SUBMIT_RUN = "submit_run"
    RUN_NOW = "run_now"


class DatabricksCreateReusableJobClusterOperator(BaseOperator):

    def __init__(
            self,
            new_cluster: Dict[str, Any],
            *,
            # json: Any | None = None,
            job_name: str | None = None,
            mode: ReusableClusterMode = ReusableClusterMode.SUBMIT_RUN,
            parent_notebook_dir_path: str | None = None,
            parent_notebook_name: str | None = None,
            notebook_path: str | None = None,
            timeout_seconds: int = 600,
            databricks_conn_id: str = "databricks_default",
            databricks_retry_limit: int = 3,
            databricks_retry_delay: int = 1,
            databricks_retry_args: dict[Any, Any] | None = None,
            do_xcom_push: bool = True,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.mode = mode
        self.job_name = job_name or "infinite loop"
        self.parent_notebook_name = parent_notebook_name
        self.parent_notebook_dir_path = parent_notebook_dir_path
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_conn_id = databricks_conn_id
        self.new_cluster = new_cluster
        self.timeout_seconds = timeout_seconds
        self.do_xcom_push = do_xcom_push
        self.notebook_path = notebook_path
        self.run_id = None
        self.cluster_id = None

    def _get_job_json(self, notebook_path: str):
        return {
            "name": self.job_name,
            "webhook_notifications": {},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": [
                {
                    "task_key": "infinite_loop",
                    "run_if": "ALL_SUCCESS",
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "source": "WORKSPACE"
                    },
                    "new_cluster": self.new_cluster,
                    "timeout_seconds": self.timeout_seconds,
                    "email_notifications": {}
                }
            ],
            "format": "MULTI_TASK"
        }

    @lru_cache(maxsize=1)
    def _get_notebook_base64(self):
        import reusable_job_cluster.templates
        from pathlib import Path
        file = Path(reusable_job_cluster.templates.__file__).parent / "infinite_loop_notebook.template"
        with open(file, "r") as f:
            notebook_data = f.read()
        return base64.b64encode(notebook_data.encode("utf-8")).decode("utf-8")

    def _get_standard_root_dir(self):
        return str(
            Path("/Users") / Path(self._hook.get_current_user()["userName"]) / Path(".reusable_job_cluster/notebooks"))

    def _ensure_notebook_path(self):
        parent_notebook_dir_path: Path = Path(self.parent_notebook_dir_path) if self.parent_notebook_dir_path \
            else self._get_standard_root_dir()
        self._hook.do_mkdirs(str(parent_notebook_dir_path))  # ensures directory exists
        parent_notebook_name: Path = Path(self.parent_notebook_name) if self.parent_notebook_name else \
            Path("infinite_loop_reuseable_job")
        return str(parent_notebook_dir_path / parent_notebook_name)

    def _make_notebook_if_not_exists(self):
        notebook_content = self._get_notebook_base64()
        notebook_path = self._ensure_notebook_path()
        try:
            remote_content = self._hook.do_export_notebook(notebook_path)
            if remote_content != notebook_content:
                self.log.info("Framework notebook content is different so re-uploading")
                self._hook.do_import_notebook(notebook_path, notebook_content)
            else:
                self.log.info("Framework notebook content is the same so not re-uploading")
        except Exception:
            self._hook.do_import_notebook(notebook_path, notebook_content)

        return notebook_path

    @property
    def _hook(self):
        return self._get_hook(caller="DatabricksCreateReusableJobClusterOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def _get_cluster_id(self, run_id: int) -> str:
        while True:
            this_run = self._hook.get_run(run_id)
            self.log.info(f"looking into populating cluster id: {this_run}")
            if "tasks" in this_run:
                for task in this_run["tasks"]:
                    if "cluster_instance" in task:
                        return task["cluster_instance"]["cluster_id"]
            time.sleep(5)

    def execute_in_submit_run_mode(self, json_normalised):
        json_normalised["run_name"] = json_normalised.get("name", "infinite_loop_cluster_reuse_run")
        self.run_id = self._hook.submit_run(json_normalised)
        self.cluster_id = self._get_cluster_id(self.run_id)
        self.log.info("Run submitted with run_id: %s and cluster_id: %s", self.run_id, self.cluster_id)

    def _default_tags(self):
        return {
            "dag_id": self.dag_id,
            "created_for": "airflow",
            "created_by": self._hook.get_current_user()["userName"],
            "purpose": "reusable_job_cluster",
        }

    def execute_in_run_now_mode(self, json_normalised):
        job_id = self._hook.find_job_id_by_name(self.job_name)
        # populate some default tags
        json_normalised["tags"] = {**(json_normalised.get("tags", {})), **self._default_tags()}
        if job_id is None:
            job_id = self._hook.create_job(json_normalised)
        else:
            self._hook.reset_job(job_id, json_normalised)

        self.run_id = self._hook.run_now({"job_id": job_id})
        self.cluster_id = self._get_cluster_id(self.run_id)
        self.log.info("Run now with job_id: %s, run_id: %s and cluster_id: %s", job_id,
                      self.run_id,
                      self.cluster_id)

    def execute(self, context: Context):
        self.log.info("Started Execute in mode: %s", self.mode)
        notebook_path = self._make_notebook_if_not_exists() if self.notebook_path is None else self.notebook_path
        json_normalised = normalise_json_content(self._get_job_json(notebook_path))
        if self.mode == ReusableClusterMode.SUBMIT_RUN:
            self.execute_in_submit_run_mode(json_normalised)
        if self.mode == ReusableClusterMode.RUN_NOW:
            self.execute_in_run_now_mode(json_normalised)
        if context is not None:
            context["ti"].xcom_push(key=XCOM_PARENT_RUN_ID_KEY, value=self.run_id)
            context["ti"].xcom_push(key=XCOM_INFINITE_LOOP_CLUSTER_ID, value=self.cluster_id)

    def on_kill(self):
        if self.run_id:
            self._hook.cancel_run(self.run_id)
            self.log.info(
                "Task: %s with run_id: %s was requested to be cancelled.", self.task_id, self.run_id
            )
        else:
            self.log.error("Error: Task: %s with invalid run_id was requested to be cancelled.", self.task_id)


class DatabricksDestroyReusableJobClusterOperator(BaseOperator):

    def __init__(
            self,
            *,
            job_create_task_id: str,
            databricks_conn_id: str = "databricks_default",
            databricks_retry_limit: int = 3,
            databricks_retry_delay: int = 1,
            databricks_retry_args: dict[Any, Any] | None = None,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_create_task_id = job_create_task_id
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_conn_id = databricks_conn_id

    @property
    @lru_cache(maxsize=1)
    def _hook(self):
        return self._get_hook(caller="DatabricksDestroyReusableJobClusterOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        run_id = context["ti"].xcom_pull(task_ids=self.job_create_task_id, key=XCOM_PARENT_RUN_ID_KEY)
        self._hook.cancel_run(run_id)


class DatabricksResizeReusableJobClusterOperator(BaseOperator):

    def __init__( self, 
                 *,
                job_create_task_id: str,
                 databricks_conn_id: str = "databricks_default",
                 num_workers = None,
                autoscale_args: dict[Any, Any] | None = None,
                max_retries = 60,
                 **kwargs
    ):
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.num_workers = num_workers
        self.job_create_task_id = job_create_task_id
        self.max_retries = max_retries
        self.autoscale_args = autoscale_args

        if num_workers is not None and autoscale_args is not None:
            raise AirflowException(f"Both num_workers ({num_workers}) and autoscale_args ({autoscale_args}) cannot be provided at the same time.")
        else:
            self.num_workers = num_workers
            self.autoscale_args = autoscale_args

    @property
    @lru_cache(maxsize=1)
    def _hook(self):
        return self._get_hook(caller="DatabricksResizeReusableJobClusterOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            caller=caller,
        )

    def wait_for_running_state(self, cluster_id: str):
        
        retry_count = 0
        while retry_count < self.max_retries:
            cluster_info = self._hook.get_cluster_info(cluster_id)

            if cluster_info["state"] == "RUNNING":
                self.log.info(f"Cluster {cluster_id} is in running state.")
                return True
            else:
                if cluster_info["state"] not in  ["PENDING", "RESIZING", "RESTARTING"]:
                    raise AirflowException(f"Job cluster {cluster_id} is in an unacceptable state and unable to resize. Current state: {cluster_info['state']}")
            
            self.log.info(f"Cluster {cluster_id} is currently in a : {cluster_info['state']} state. Retrying in 10 seconds.")
            retry_count += 1

            time.sleep(10)

        self.log.info(f"Cluster {cluster_id} failed to transition to RUNNING state in {self.max_retries * 10} seconds and is currently in a : {cluster_info['state']} state.")
        return False

    def execute(self, context: Context):
        cluster_id = context["ti"].xcom_pull(task_ids=self.job_create_task_id, key=XCOM_INFINITE_LOOP_CLUSTER_ID)
        
        if self.wait_for_running_state(cluster_id):
            if self.num_workers is not None:
                json = {
                    "cluster_id": cluster_id,
                    "num_workers": self.num_workers,
                }
            else:
                json = {
                    "cluster_id": cluster_id,
                    "autoscale": self.autoscale_args,
                }

            self.log.info(f"json --> : {json}")
            self._hook.resize_cluster(json)
        else:
            raise AirflowException("job cluster did not transition into a running state; unable to resize.") 


class ReuseableJobClusterBuilder:
    def __init__(self):
        self._job_cluster = DatabricksReusableJobCluster()

    def with_new_cluster(self, new_cluster: Dict[str, Any]):
        self._job_cluster.new_cluster = new_cluster
        return self

    def with_dag(self, dag: DAG):
        self._job_cluster.dag = dag
        return self

    def with_task_prefix(self, task_prefix: str = "databricks_reusable_job_cluster"):
        self._job_cluster.task_prefix = task_prefix
        return self

    def with_create_op_kwargs(self, create_op_kwargs: Dict[str, Any]):
        self._job_cluster.create_op_kwargs = create_op_kwargs
        return self

    def with_delete_op_kwargs(self, delete_op_kwargs: Dict[str, Any]):
        self._job_cluster.delete_op_kwargs = delete_op_kwargs
        return self

    def with_parent_notebook_dir_path(self, parent_notebook_dir_path: str):
        self._job_cluster.parent_notebook_dir_path = parent_notebook_dir_path
        return self

    def with_parent_notebook_name(self, parent_notebook_name: str):
        self._job_cluster.parent_notebook_name = parent_notebook_name
        return self

    def with_notebook_path(self, notebook_path: str):
        self._job_cluster.notebook_path = notebook_path
        return self

    def with_timeout_seconds(self, timeout_seconds: int):
        self._job_cluster.timeout_seconds = timeout_seconds
        return self

    def with_databricks_conn_id(self, databricks_conn_id: str):
        self._job_cluster.databricks_conn_id = databricks_conn_id
        return self

    def with_databricks_retry_limit(self, databricks_retry_limit: int):
        self._job_cluster.databricks_retry_limit = databricks_retry_limit
        return self

    def with_databricks_retry_delay(self, databricks_retry_delay: int):
        self._job_cluster.databricks_retry_delay = databricks_retry_delay
        return self

    def with_databricks_retry_args(self, databricks_retry_args: Dict[Any, Any]):
        self._job_cluster.databricks_retry_args = databricks_retry_args
        return self

    def with_do_xcom_push(self, do_xcom_push: bool):
        self._job_cluster.do_xcom_push = do_xcom_push
        return self

    def with_job_prefix_and_suffix(self, *, job_prefix: str, job_suffix: str):
        self._job_cluster.job_prefix = job_prefix
        self._job_cluster.job_suffix = job_suffix
        return self

    def with_run_now_mode(self):
        self._job_cluster.mode = ReusableClusterMode.RUN_NOW
        return self

    def build_operators(self) -> Tuple[
        DatabricksCreateReusableJobClusterOperator,
        DatabricksDestroyReusableJobClusterOperator, str]:
        self._job_cluster.validate()
        return (
            self._job_cluster.to_create_operator(),
            self._job_cluster.to_destroy_operator(),
            self._job_cluster.existing_cluster_id()
        )


class DatabricksReusableJobCluster:

    def __init__(self,
                 *,
                 task_prefix: str | None = None,
                 timeout_seconds: int | None = None,
                 job_prefix: str | None = None,
                 job_suffix: str | None = None,
                 mode: ReusableClusterMode = ReusableClusterMode.SUBMIT_RUN,
                 dag: DAG | None = None,
                 new_cluster: dict[str, Any] | None = None,
                 parent_notebook_dir_path: str | None = None,
                 parent_notebook_name: str | None = None,
                 notebook_path: str | None = None,
                 databricks_conn_id: str = "databricks_default",
                 databricks_retry_limit: int = 3,
                 databricks_retry_delay: int = 1,
                 databricks_retry_args: dict[Any, Any] | None = None,
                 do_xcom_push: bool = True,
                 create_op_kwargs: dict[Any, Any] | None = None,
                 delete_op_kwargs: dict[Any, Any] | None = None, ):
        self.mode = mode
        self.job_suffix = job_suffix
        self.job_prefix = job_prefix
        self.dag = dag
        self.delete_op_kwargs = delete_op_kwargs or {}
        self.create_op_kwargs = create_op_kwargs or {}
        self.task_prefix = task_prefix
        self.do_xcom_push = do_xcom_push
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        self.notebook_path = notebook_path
        self.parent_notebook_name = parent_notebook_name
        self.parent_notebook_dir_path = parent_notebook_dir_path
        self.new_cluster = new_cluster

    @staticmethod
    def builder():
        return ReuseableJobClusterBuilder()

    def validate(self):
        validation_errors = []

        if self.dag is None:
            validation_errors.append("dag cannot be None; use with_dag() to set dag")
        if self.new_cluster is None:
            validation_errors.append("new_cluster cannot be None; use with_new_cluster() to set new_cluster")
        if self.task_prefix is None:
            validation_errors.append("task_prefix cannot be None; use with_task_prefix() to set task_prefix")
        if self.timeout_seconds is None:
            validation_errors.append(
                "timeout_seconds cannot be None; use with_timeout_seconds() to set timeout_seconds")

        # This check is there to avoid creating too many duplicate jobs for just one airflow dag
        if self.dag is not None and self.mode == ReusableClusterMode.RUN_NOW:
            for task in self.dag.tasks:
                if isinstance(task, DatabricksCreateReusableJobClusterOperator):
                    validation_errors.append("One dag can only have one instance of "
                                             "DatabricksCreateReusableJobClusterOperator in RUN_NOW mode")

        if validation_errors:
            error_message = "\n".join(validation_errors)
            raise ValueError(error_message)

    def to_dict(self):
        payload = self.__dict__.copy()
        pop_fields = ["delete_op_kwargs", "create_op_kwargs", "dag", "task_prefix", "job_suffix", "job_prefix"]
        for field in pop_fields:
            payload.pop(field, None)
        return payload

    @property
    def create_task_id(self):
        return f"{self.task_prefix}_create"

    @property
    def job_name(self):
        # replace all hyphens, spaces, and periods with _ to avoid issues with databricks job name
        job_name_parts = [value for value in [self.job_prefix, self.dag.dag_id, self.job_suffix] if value is not None]
        job_name = "_".join(job_name_parts)
        return job_name.translate(str.maketrans("-. ", "___"))

    def existing_cluster_id(self):
        formatted_string = f"{{{{ task_instance.xcom_pull(task_ids='{self.create_task_id}', " \
                           f"key='{XCOM_INFINITE_LOOP_CLUSTER_ID}') }}}}"
        return formatted_string

    def to_create_operator(self) -> DatabricksCreateReusableJobClusterOperator:
        return DatabricksCreateReusableJobClusterOperator(
            dag=self.dag,
            task_id=self.create_task_id,
            job_name=self.job_name,
            **self.to_dict(),
            **self.create_op_kwargs)

    def to_destroy_operator(self) -> DatabricksDestroyReusableJobClusterOperator:
        # hard coded values for now since the list of args are very small
        return DatabricksDestroyReusableJobClusterOperator(
            dag=self.dag,
            task_id=f"{self.task_prefix}_destroy",
            job_create_task_id=self.create_task_id,
            databricks_conn_id=self.databricks_conn_id,
            databricks_retry_limit=self.databricks_retry_limit,
            databricks_retry_delay=self.databricks_retry_delay,
            databricks_retry_args=self.databricks_retry_args,
            **self.delete_op_kwargs
        )
