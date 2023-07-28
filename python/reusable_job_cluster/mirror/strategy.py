import abc
import copy
import functools
import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from databricks.sdk.service import iam
from databricks.sdk.service.jobs import Task, TaskDependency, NotebookTask, ConditionTask, ConditionTaskOp, Run, RunNow, \
    JobCluster, JobEmailNotifications, JobNotificationSettings, \
    WebhookNotifications, JobRunAs, CreateJob

from reusable_job_cluster.mirror import GlobalJobParameters, TaskSpecificParameters, PollerTaskValues
from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook, RunState


def check_task_name(task_name: str):
    return f"{task_name}_check"


def conditional_task_name(task_name: str):
    return f"{task_name}_conditional"


def get_job_param(param: str):
    return f"{{{{job.parameters.{param}}}}}"


def get_job_global_task_params() -> Dict[str, str]:
    res = {}
    for param in GlobalJobParameters:
        res[param.value] = get_job_param(param.value)
    return res


@dataclass
class AirflowDagToDatabricksTask:
    task_data: Dict[str, Any]
    task_name: str
    airflow_dag_id: str
    airflow_task_id: str
    airflow_trigger_rule: str
    depends_on_databricks_task: List[str] = None
    depends_on: List[str] = None
    remove_fields: List[str] = None
    conditional_value_key: str = PollerTaskValues.RESULT_STATE_KEY.value

    def get_check_task_name(self):
        return check_task_name(self.task_name)

    def get_conditional_task_name(self):
        return conditional_task_name(self.task_name)

    @classmethod
    def from_databricks_submit_operator(cls, dag: DAG, op: DatabricksSubmitRunOperator):
        op_json = copy.deepcopy(op.json)
        databricks_task_ids = [task_id for task_id in op.get_flat_relative_ids(upstream=True) if
                               isinstance(dag.get_task(task_id), DatabricksSubmitRunOperator)]
        return cls(
            airflow_trigger_rule=op.trigger_rule.value,
            airflow_dag_id=dag.dag_id,
            airflow_task_id=op.task_id,
            depends_on=list(op.upstream_task_ids),
            depends_on_databricks_task=databricks_task_ids,
            task_data=op_json,
            task_name=op.task_id
        )

    def _make_cluster_info(self, cluster_key: str = None,
                           existing_cluster_id: str = None) -> Dict[str, str]:
        return {"existing_cluster_id": existing_cluster_id} if existing_cluster_id is not None else \
            {"job_cluster_key": cluster_key}

    def _make_actual_submit_task(self,
                                 cluster_info: Dict[str, str]) -> Task:
        remove_fields = self.remove_fields or \
                        ["run_name", "idempotency_token", "access_control_list", "existing_cluster_id", "new_cluster"]
        submit_data = {k: v for k, v in self.task_data.items() if k not in remove_fields}
        return Task.from_dict({**{"task_key": self.task_name,
                                  **cluster_info,
                                  "depends_on": [
                                      {
                                          "task_key": self.get_conditional_task_name(),
                                          "outcome": "true"
                                      }
                                  ]},
                               **submit_data})

    def _make_polling_task(self,
                           notebook_path,
                           cluster_info: Dict[str, str]
                           ):
        depends_on = {} if self.depends_on_databricks_task is None else {"depends_on": [
            TaskDependency(task_key=check_task_name(task_id)) for task_id in self.depends_on_databricks_task]}
        return Task(
            task_key=self.get_check_task_name(),
            **cluster_info,
            **depends_on,
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters={
                    TaskSpecificParameters.UPSTREAM_TASK_IDS.value: json.dumps(self.depends_on),
                    TaskSpecificParameters.TASK_ID.value: self.airflow_task_id,
                    TaskSpecificParameters.TRIGGER_RULE.value: self.airflow_trigger_rule,
                    GlobalJobParameters.DAG_ID.value: "NONE",
                    GlobalJobParameters.DAG_RUN_ID.value: "NONE",
                }
            )
        )

    def _make_conditional_task(self) -> Task:
        left = f"tasks.{self.get_check_task_name()}.values.{self.conditional_value_key}"
        # conditional tasks dont need clusters
        return Task(
            task_key=self.get_conditional_task_name(),
            depends_on=[TaskDependency(task_key=self.get_check_task_name())],
            condition_task=ConditionTask(left="{{" + left + "}}",
                                         op=ConditionTaskOp.EQUAL_TO,
                                         right="SUCCESS")
        )

    def make_tasks(self, polling_notebook_path: str,
                   cluster_key: str = None,
                   existing_cluster_id: str = None
                   ) -> List[Task]:
        cluster_info = self._make_cluster_info(cluster_key, existing_cluster_id)
        return [
            self._make_polling_task(polling_notebook_path, cluster_info),
            self._make_conditional_task(),  # conditional tasks dont need clusters
            self._make_actual_submit_task(cluster_info)
        ]


class WorkflowHelper:

    def __init__(self, databricks_dag_to_workflow_strategy: "ClusterReuseStrategy",
                 hook: DatabricksHook
                 ):
        self._hook = hook
        self._databricks_dag_to_workflow_strategy = databricks_dag_to_workflow_strategy

    def get_job_id_by_name(self, fail_if_more_than_one: bool = False) -> Optional[int]:
        jobs = self._hook.list_jobs(job_name=self._databricks_dag_to_workflow_strategy.get_job_name())
        if fail_if_more_than_one is True and len(jobs) > 1:
            raise ValueError(f"More than one job found with name "
                             f"{self._databricks_dag_to_workflow_strategy.get_job_name()}")
        if len(jobs) == 0:
            return None
        return jobs[0]["job_id"]

    def run_now(self, payload: RunNow = None, notebook_params: Dict[str, Any] = None) -> int:
        payload = payload or RunNow(job_id=self.get_job_id_by_name())
        if notebook_params is not None:
            payload.notebook_params = notebook_params
        return self._hook.run_now(payload.as_dict())

    def list_active_runs(self, by_job_name=True) -> List[Run]:
        if by_job_name is True:
            runs = self._hook.list_all_runs_by_job(self.get_job_id_by_name(), active_only=True).get("runs", [])
            return [Run.from_dict(run) for run in runs]
        raise NotImplementedError()

    def get_task_status(self, run_id: int, task_key: str) -> Optional[RunState]:
        tasks = self._hook.get_run(run_id).get("tasks", [])
        for t in tasks:
            if t.get("task_key") == task_key:
                return RunState.from_json(json.dumps(t.get("state")))
        return None

    @functools.lru_cache(maxsize=1024)
    def get_task_url(self, run_id: int, task_key: Optional[str] = None) -> Optional[str]:
        run = self._hook.get_run(run_id)
        tasks = run.get("tasks", [])
        if task_key is None:
            return run.get("run_page_url")
        for t in tasks:
            if t.get("task_key") == task_key:
                task_run_id = t.get("run_id")
                task_run = self._hook.get_run(task_run_id)
                return task_run.get("run_page_url")
        return None


# TODO: refactor there are not multiple strategies, we can do this abstraction later
class ClusterReuseStrategy(abc.ABC):

    def __init__(self,
                 job_clusters: List[JobCluster] = None,
                 existing_cluster_id: str = None,  # for testing
                 cluster_key_task_mapping: Dict[str, str] = None):
        self._existing_cluster_id = existing_cluster_id
        self._cluster_key_task_mapping = cluster_key_task_mapping
        self._job_clusters = job_clusters

    @abc.abstractmethod
    def idempotent_create_workflow_and_assets(self, hook: DatabricksHook):
        pass

    @abc.abstractmethod
    def get_job_name(self):
        pass

    @abc.abstractmethod
    def orphan_databricks_tasks(self) -> List[str]:
        pass


def get_airflow_dag_to_databricks_tasks(dag: DAG) -> List[AirflowDagToDatabricksTask]:
    tasks = []
    for task in dag.tasks:
        if isinstance(task, DatabricksSubmitRunOperator):
            tasks.append(AirflowDagToDatabricksTask.from_databricks_submit_operator(dag, task))
    return tasks


class DatabricksMirroredWorkflow(ClusterReuseStrategy):

    def __init__(self,
                 name: str,
                 tasks: List[AirflowDagToDatabricksTask],
                 job_clusters: List[JobCluster] = None,
                 existing_cluster_id: str = None,
                 cluster_task_mapping: Dict[str, str] = None,
                 email_notifications: JobEmailNotifications = None,
                 notification_settings: JobNotificationSettings = None,
                 job_run_as: JobRunAs = None,
                 tags: Dict[str, str] = None,
                 timeout_seconds: int = None,
                 max_concurrent_runs: int = None,
                 webhook_notifications: WebhookNotifications = None,
                 access_control_list: Optional[List[iam.AccessControlRequest]] = None
                 ):
        super().__init__(job_clusters, cluster_task_mapping)
        self._max_concurrent_runs = max_concurrent_runs
        self._existing_cluster_id = existing_cluster_id
        self._access_control_list = access_control_list
        self._job_run_as = job_run_as
        self._webhook_notifications = webhook_notifications
        self._timeout_seconds = timeout_seconds
        self._tags = tags
        self._notification_settings = notification_settings
        self._email_notifications = email_notifications
        self._name = name
        self._tasks: List[AirflowDagToDatabricksTask] = tasks
        self._polling_notebook_path = None

    @property
    def name(self):
        return self._name

    def orphan_databricks_tasks(self) -> List[str]:
        return [task.task_name for task in self._tasks if not task.depends_on_databricks_task]

    def set_polling_notebook_path(self, polling_notebook_path: str):
        self._polling_notebook_path = polling_notebook_path

    def idempotent_create_workflow_and_assets(self, hook: DatabricksHook):
        if self._polling_notebook_path is None:
            raise ValueError("Polling notebook path is None")

        # TODO: refactor
        job_settings = self.get_job_settings()
        api_helper = WorkflowHelper(self, hook)

        job_id = api_helper.get_job_id_by_name()
        if job_id is None:
            return hook.create_job(job_settings.as_dict())

        return hook.reset_job(job_id, job_settings.as_dict())

    def get_job_name(self):
        return f"[airflow] {self._name}"

    def get_job_settings(self) -> CreateJob:
        tasks = []

        for task in self._tasks:
            no_existing_cluster_or_mapping = self._existing_cluster_id is None and \
                                             self._cluster_key_task_mapping is None
            cluster_key = self._job_clusters[0].job_cluster_key if no_existing_cluster_or_mapping else \
                (self._cluster_key_task_mapping or {}).get(task.task_name)
            tasks.extend(task.make_tasks(self._polling_notebook_path, cluster_key, self._existing_cluster_id))

        return CreateJob(
            access_control_list=self._access_control_list,
            max_concurrent_runs=self._max_concurrent_runs,
            name=self.get_job_name(),
            job_clusters=self._job_clusters,
            tasks=tasks,
            timeout_seconds=self._timeout_seconds,
            tags=self._tags,
            email_notifications=self._email_notifications,
            notification_settings=self._notification_settings,
            webhook_notifications=self._webhook_notifications,
            run_as=self._job_run_as,

        )
