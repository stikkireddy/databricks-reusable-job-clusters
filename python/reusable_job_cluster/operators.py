from __future__ import annotations

import base64
from functools import lru_cache
import time
from pathlib import Path

from typing import TYPE_CHECKING, Any, Dict

from airflow.models import BaseOperator

from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook
from reusable_job_cluster.vendor.utils.databricks import normalise_json_content

if TYPE_CHECKING:
    from airflow.utils.context import Context

XCOM_PARENT_RUN_ID_KEY = "parent_run_id"
XCOM_INFINITE_LOOP_CLUSTER_ID = "infinite_loop_cluster_id"

class DatabricksCreateReusableJobClusterOperator(BaseOperator):

    def __init__(
            self,
            new_cluster: Dict[str, Any],
            *,
            # json: Any | None = None,
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
            "name": "infinite loop",
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
        return str(Path("/Users") / Path(self._hook.get_current_user()["userName"]) / Path(".reusable_job_cluster/notebooks"))

    def _ensure_notebook_path(self):
        parent_notebook_dir_path: Path = Path(self.parent_notebook_dir_path) if self.parent_notebook_dir_path \
            else self._get_standard_root_dir()
        self._hook.do_mkdirs(str(parent_notebook_dir_path)) # ensures directory exists
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
                self._hook.do_import_notebook(self._ensure_notebook_path(), notebook_content)
            else:
                self.log.info("Framework notebook content is the same so not re-uploading")
        except Exception:
            self._hook.do_import_notebook(self._ensure_notebook_path(), notebook_content)

        return notebook_path



    @property
    @lru_cache(maxsize=1)
    def _hook(self):
        return self._get_hook(caller="DatabricksSubmitRunOperator")

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

    def execute(self, context: Context):
        self.log.info("Started Execute")
        notebook_path = self._make_notebook_if_not_exists()
        json_normalised = normalise_json_content(self._get_job_json(notebook_path))
        self.run_id = self._hook.submit_run(json_normalised)
        self.cluster_id = self._get_cluster_id(self.run_id)
        self.log.info("Run submitted with run_id: %s and cluster_id: %s", self.run_id, self.cluster_id)
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
        return self._get_hook(caller="DatabricksSubmitRunOperator")

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
