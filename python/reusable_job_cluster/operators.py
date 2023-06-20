from __future__ import annotations

import base64
import re
import time
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Tuple, Optional, List

import networkx as nx
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook
from reusable_job_cluster.vendor.utils.databricks import normalise_json_content

if TYPE_CHECKING:
    from airflow.utils.context import Context

XCOM_PARENT_RUN_ID_KEY = "parent_run_id"
XCOM_INFINITE_LOOP_CLUSTER_ID = "infinite_loop_cluster_id"


class ReusableClusterMode(Enum):
    SUBMIT_RUN = "submit_run"
    RUN_NOW = "run_now"


def convert_non_alphanumeric_to_underscore(text):
    pattern = r'[^a-zA-Z0-9]+'
    converted_text = re.sub(pattern, '_', text)
    return converted_text.strip('_')


def convert_email_to_underscore(text):
    first_part = text.split('@')[0]
    return first_part.replace(".", "_")


class DatabricksCreateReusableJobClusterOperator(BaseOperator):

    def __init__(
            self,
            new_cluster: Dict[str, Any],
            *,
            # json: Any | None = None,
            job_name: str | None = None,
            mode: ReusableClusterMode = ReusableClusterMode.SUBMIT_RUN,
            tags: Optional[Dict[str, str]] = None,
            access_control_list: Optional[List[Dict[str, str]]] = None,
            parent_notebook_dir_path: str | None = None,
            parent_notebook_name: str | None = None,
            notebook_path: str | None = None,
            timeout_seconds: Optional[int] = 600,
            databricks_conn_id: str = "databricks_default",
            databricks_retry_limit: int = 3,
            databricks_retry_delay: int = 1,
            databricks_retry_args: dict[Any, Any] | None = None,
            do_xcom_push: bool = True,
            **kwargs,
    ):
        super().__init__(**kwargs)
        self.access_control_list = access_control_list
        self.tags = tags or {}  # default tags is None
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
        job_config = {
            "name": self.job_name,
            "webhook_notifications": {},
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
                    "email_notifications": {}
                }
            ],
            "format": "MULTI_TASK"
        }
        if self.timeout_seconds is not None:
            job_config["timeout_seconds"] = self.timeout_seconds
        if self.access_control_list is not None and len(self.access_control_list) > 0:
            # access_control_list length must be greater than 0
            job_config["access_control_list"] = self.access_control_list
        return job_config

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
            "dag_id": convert_non_alphanumeric_to_underscore(self.dag_id),
            "created_for": "airflow",
            "created_by": convert_email_to_underscore(self._hook.get_current_user()["userName"]),
            "purpose": "reusable_job_cluster",
        }

    def get_normalized_tags(self, json_normalised: Dict[str, Any]):
        return {**(json_normalised.get("tags", {})), **self._default_tags(), **self.tags}

    def execute_in_run_now_mode(self, json_normalised):
        job_id = self._hook.find_job_id_by_name(self.job_name)
        # populate some default tags
        json_normalised["tags"] = self.get_normalized_tags(json_normalised)
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
        # modify tags the moment execute kicks off
        self.new_cluster["custom_tags"] = {
            **self._default_tags(),
            **self.tags,
            **(self.new_cluster.get("custom_tags", {}))
        }
        notebook_path = self._make_notebook_if_not_exists() if self.notebook_path is None else self.notebook_path
        json_normalised = normalise_json_content(self._get_job_json(notebook_path))
        if self.mode == ReusableClusterMode.SUBMIT_RUN:
            # add custom tags to the job during submit run
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
        if run_id is None:
            self.log.info("Unable to find run_id in XCOM so succeeding!")
            return
        self._hook.cancel_run(run_id)


def convert_to_list_if_string(value: Optional[str | List[str]]) -> List[str]:
    """
    Converts a string value to a list containing that value if it is a string.
    If the value is already a list, it is returned as is.
    """
    if value is None:
        return []

    if isinstance(value, str):
        return [value]
    elif isinstance(value, list):
        return value

    return []


class ReuseableJobClusterBuilder:
    def __init__(self):
        self._job_cluster = DatabricksReusableJobCluster()

    def with_new_cluster(self, new_cluster: Dict[str, Any]):
        self._job_cluster.new_cluster = new_cluster
        return self

    def with_tags(self, tags: Dict[str, Any]):
        self._job_cluster.tags = tags
        return self

    def _with_permissions(self, permission_level: str, *,
                          user_names: Optional[List[str]] = None,
                          group_names: Optional[List[str]] = None,
                          service_principal_names: Optional[List[str]] = None):
        acls = []
        users = convert_to_list_if_string(user_names)
        for user in users:
            acls.append({"user_name": user, "permission_level": permission_level})
        groups = convert_to_list_if_string(group_names)
        for group in groups:
            acls.append({"group_name": group, "permission_level": permission_level})
        service_principals = convert_to_list_if_string(service_principal_names)
        for service_principal in service_principals:
            acls.append({"service_principal_name": service_principal, "permission_level": permission_level})
        self._job_cluster.access_control_list += acls
        return self

    def with_view_permissions(self, *,
                              user_names: Optional[List[str]] = None,
                              group_names: Optional[List[str]] = None,
                              service_principal_names: Optional[List[str]] = None):
        return self._with_permissions("CAN_VIEW", user_names=user_names, group_names=group_names,
                                      service_principal_names=service_principal_names)

    def with_manage_permissions(self, *,
                                user_names: Optional[List[str]] = None,
                                group_names: Optional[List[str]] = None,
                                service_principal_names: Optional[List[str]] = None):
        return self._with_permissions("CAN_MANAGE", user_names=user_names, group_names=group_names,
                                      service_principal_names=service_principal_names)

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

    def with_timeout_seconds(self, timeout_seconds: Optional[int]):
        self._job_cluster.timeout_seconds = timeout_seconds
        return self

    def without_destroy_cluster_on_any_failure(self):
        self._job_cluster.destroy_cluster_on_failure = False
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

    def build_operators(self, autowire: bool = False,
                        destroy_only_when_all_done: bool = True,
                        create_op_parent_task_list: Optional[
                            str | List[str] | BaseOperator | List[BaseOperator]] = None) -> \
            Tuple[
                DatabricksCreateReusableJobClusterOperator,
                DatabricksDestroyReusableJobClusterOperator, str]:
        self._job_cluster.validate()
        if autowire is True:
            create_op = self._job_cluster.to_create_operator()
            destroy_op = self._job_cluster.to_destroy_operator()
            create_op_upstream_tasks, destroy_op_downstream_tasks = \
                self.autowire(self._job_cluster.dag, self._job_cluster.existing_cluster_id(),
                              create_op.task_id, destroy_op.task_id, destroy_only_when_all_done)
            for task in create_op_upstream_tasks:
                create_op >> self._job_cluster.dag.get_task(task)
            for task in destroy_op_downstream_tasks:
                self._job_cluster.dag.get_task(task) >> destroy_op
            if create_op_parent_task_list is not None:
                parent_task_list = create_op_parent_task_list if isinstance(create_op_parent_task_list, list) \
                    else [create_op_parent_task_list]
                for parent_task in parent_task_list:
                    if isinstance(parent_task, str):
                        self._job_cluster.dag.get_task(parent_task) >> create_op
                    elif isinstance(parent_task, BaseOperator):
                        parent_task >> create_op
        else:
            return (
                self._job_cluster.to_create_operator(),
                self._job_cluster.to_destroy_operator(),
                self._job_cluster.existing_cluster_id()
            )

    def modify_databricks_run_submit_task(self, task: "DatabricksSubmitRunOperator", existing_cluster_id: str):
        task.existing_cluster_id = existing_cluster_id
        task.json["existing_cluster_id"] = existing_cluster_id
        task.json.pop("new_cluster", None)

    def autowire(self, dag: DAG, existing_cluster_id: str, create_op_name, destroy_op_name, destroy_only_when_all_done):
        graph = nx.DiGraph()
        graph.add_node("root")  # dummy root node
        graph.add_node(create_op_name)
        graph.add_node(destroy_op_name)
        for task in dag.tasks:
            graph.add_node(task.task_id, klass=task.operator_class)
            graph.add_edge(task.task_id, "root", )  # all tasks are downstream of root
            if task.operator_class.__name__ == "DatabricksSubmitRunOperator":
                graph.add_edge(task.task_id, create_op_name)  # all Submit Runs are upstream of create op
                graph.add_edge(destroy_op_name, task.task_id)  # all Submit Runs are downstream of destroy op
                self.modify_databricks_run_submit_task(task, existing_cluster_id)
        for task in dag.tasks:
            for task_id in task.upstream_task_ids:
                graph.add_edge(task.task_id, task_id)

        # transitive reduction to simplify graph
        if destroy_only_when_all_done is True:
            # connect to everything for destroy and then simplify for create
            destroy_list = list(graph.successors(destroy_op_name))
            graph = nx.transitive_reduction(graph)
        else:
            graph = nx.transitive_reduction(graph)
            # simplify for everything and there may be straggler tasks that are not connected to destroy
            destroy_list = list(graph.successors(destroy_op_name))

        create_list = list(graph.predecessors(create_op_name))

        if "root" in create_list:
            create_list.remove("root")  # remove dummy root node

        if "root" in destroy_list:
            destroy_list.remove("root")  # remove dummy root node

        # first item is a list of task ids that should be upstream of the create op
        # second item is a list of task ids that should be downstream of the destroy op
        # this allows the dag to look very clean and simple to read
        return create_list, destroy_list


class DatabricksReusableJobCluster:

    def __init__(self,
                 *,
                 access_control_list: List[dict[str, str]] | None = None,
                 task_prefix: str | None = None,
                 timeout_seconds: int | None = -999,    # -999 means the value is unset
                 destroy_cluster_on_failure: bool = True,
                 job_prefix: str | None = None,
                 job_suffix: str | None = None,
                 tags: Dict[str, str] | None = None,
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
        self.access_control_list = access_control_list or []
        self.tags = tags
        self.destroy_cluster_on_failure = destroy_cluster_on_failure
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

    def check_duplicate_principals(self):
        encountered_users = set()
        encountered_groups = set()
        encountered_service_principals = set()
        errors = []

        for acl in self.access_control_list:
            user_name = acl.get('user_name')
            group_name = acl.get('group_name')
            service_principal_name = acl.get('service_principal_name')

            if user_name:
                if user_name in encountered_users:
                    errors.append(f"Duplicate user principal: {user_name}; acl: {acl}")
                encountered_users.add(user_name)

            if group_name:
                if group_name in encountered_groups:
                    errors.append(f"Duplicate group principal: {group_name}; acl: {acl}")
                encountered_groups.add(group_name)

            if service_principal_name:
                if service_principal_name in encountered_service_principals:
                    errors.append(f"Duplicate service principal: {service_principal_name}; acl: {acl}")
                encountered_service_principals.add(service_principal_name)
        return errors

    def validate(self):
        validation_errors = []

        if self.dag is None:
            validation_errors.append("dag cannot be None; use with_dag() to set dag")
        if self.new_cluster is None:
            validation_errors.append("new_cluster cannot be None; use with_new_cluster() to set new_cluster")
        if self.task_prefix is None:
            validation_errors.append("task_prefix cannot be None; use with_task_prefix() to set task_prefix")
        if self.tags is None:
            validation_errors.append("tags cannot be None; use with_tags() to set tags; tags are important "
                                     "for cost attribution")
        if self.timeout_seconds == -999:
            validation_errors.append("timeout_seconds cannot be unset; "
                                     "use with_timeout_seconds() to set timeout_seconds; "
                                     "it helps avoid unnecessary cost during task failures")
        validation_errors += self.check_duplicate_principals()

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
        pop_fields = ["delete_op_kwargs",
                      "create_op_kwargs",
                      "dag",
                      "destroy_cluster_on_failure",
                      "task_prefix",
                      "job_suffix",
                      "job_prefix"]
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
        additional_kwargs = {}
        if self.destroy_cluster_on_failure is True:
            additional_kwargs["trigger_rule"] = TriggerRule.ALL_DONE

        return DatabricksDestroyReusableJobClusterOperator(
            dag=self.dag,
            task_id=f"{self.task_prefix}_destroy",
            job_create_task_id=self.create_task_id,
            databricks_conn_id=self.databricks_conn_id,
            databricks_retry_limit=self.databricks_retry_limit,
            databricks_retry_delay=self.databricks_retry_delay,
            databricks_retry_args=self.databricks_retry_args,
            **additional_kwargs,
            **self.delete_op_kwargs
        )
