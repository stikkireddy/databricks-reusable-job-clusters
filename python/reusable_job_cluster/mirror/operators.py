from __future__ import annotations

import json
import time
from typing import Optional, Any, List, Dict, Sequence

from airflow import DAG
from airflow.models import BaseOperator, XCom, BaseOperatorLink
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.context import Context
from databricks.sdk.service import iam
from databricks.sdk.service.jobs import JobCluster, JobRunAs, WebhookNotifications, JobNotificationSettings, \
    JobEmailNotifications

from reusable_job_cluster.mirror import GlobalJobParameters
from reusable_job_cluster.mirror.polling import PollingNotebookHandler
from reusable_job_cluster.mirror.strategy import DatabricksMirroredWorkflow, get_airflow_dag_to_databricks_tasks
from reusable_job_cluster.mirror.strategy import WorkflowHelper, check_task_name
from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook

XCOM_PARENT_RUN_ID_KEY = "parent_run_id"
XCOM_MIRROR_RUN_PAGE_URL_KEY = "mirror_run_url"
XCOM_DEPS_CHECK_RUN_URL = "check_run_url"
XCOM_RUN_URL = "run_url"


class DatabricksMirrorRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Databricks Mirror Parent Job Run"

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_MIRROR_RUN_PAGE_URL_KEY, ti_key=ti_key)


class DatabricksDepCheckRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Databricks Dependency Task Check Run"

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_DEPS_CHECK_RUN_URL, ti_key=ti_key)


class DatabricksTaskRunLink(BaseOperatorLink):
    """Constructs a link to monitor a Databricks Job Run. This is currently not working"""

    name = "Databricks Dependency Task Check Run"

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_RUN_URL, ti_key=ti_key)


class DatabricksTaskCheckOperator(BaseOperator):
    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ("rendered_workflow", "databricks_conn_id")

    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"

    operator_extra_links = [
        DatabricksMirrorRunLink(),
        DatabricksDepCheckRunLink(),
        DatabricksTaskRunLink(),
    ]

    def __init__(
            self,
            task_name: str,
            parent_job_task_id: str,
            mirrored_workflow: DatabricksMirroredWorkflow,
            *,
            timeout_seconds: Optional[int] = 600,
            databricks_conn_id: str = "databricks_default",
            databricks_retry_limit: int = 3,
            databricks_retry_delay: int = 1,
            databricks_retry_args: dict[Any, Any] | None = None,
            do_xcom_push: bool = True,
            **kwargs,
    ):
        super().__init__(**kwargs, retries=0, )
        self.mirrored_workflow = mirrored_workflow
        self.parent_job_task_id = parent_job_task_id
        self.task_name = task_name
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        self.do_xcom_push = do_xcom_push
        self.run_id = None
        self.rendered_workflow = json.loads(json.dumps(mirrored_workflow.get_job_settings().as_dict()) \
                                            .replace('"{{', '"{% raw %}{{').replace('}}"', '{% endraw %}}}"'))

    @property
    def _hook(self):
        return self._get_hook(caller="DatabricksTaskFinishedOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    @classmethod
    def from_submit_run_operator(cls,
                                 dag: DAG,
                                 op: DatabricksSubmitRunOperator,
                                 parent_run_task_id: str,
                                 mirrored_workflow: DatabricksMirroredWorkflow
                                 ):
        return cls(
            dag=dag,
            task_id=op.task_id,
            task_name=op.task_id,
            mirrored_workflow=mirrored_workflow,
            parent_job_task_id=parent_run_task_id,
            databricks_conn_id=op.databricks_conn_id,
            databricks_retry_args=op.databricks_retry_args,
            databricks_retry_delay=op.databricks_retry_delay,
            databricks_retry_limit=op.databricks_retry_limit,
            do_xcom_push=op.do_xcom_push,
        )

    def get_run_id(self, context: Context):
        self.run_id = context['ti'].xcom_pull(task_ids=self.task_name)
        return context["ti"].xcom_pull(task_ids=self.parent_job_task_id, key=XCOM_PARENT_RUN_ID_KEY)

    def _wait_for_task_completion(self, context: Context, run_id: int, api_helper: WorkflowHelper):
        task_status = api_helper.get_task_status(run_id, self.task_name)
        task_url = api_helper.get_task_url(run_id, self.task_name)
        parent_run_url = api_helper.get_task_url(run_id)
        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_MIRROR_RUN_PAGE_URL_KEY, value=parent_run_url)
        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_RUN_URL, value=task_url)
        while task_status.is_terminal is False:
            self.log.info("Task %s is still running in state %s, follow at %s", self.task_name, task_status, task_url)
            time.sleep(5)
            task_status = api_helper.get_task_status(run_id, self.task_name)

        if task_status.is_successful is True:
            self.log.info("Task %s finished successfully", self.task_name)
        else:
            raise Exception(f"Databricks task: {self.task_name} failed")

    def _wait_for_task_check_completion(self, context: Context, run_id: int, api_helper: WorkflowHelper):
        check_task = check_task_name(self.task_name)
        task_status = api_helper.get_task_status(run_id, check_task)
        task_url = api_helper.get_task_url(run_id, check_task)
        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_DEPS_CHECK_RUN_URL, value=task_url)
        while task_status.is_terminal is False:
            self.log.info("Dependency Check Task %s is still running in state %s, follow at %s",
                          check_task, task_status, task_url)
            time.sleep(5)
            task_status = api_helper.get_task_status(run_id, check_task)

        if task_status.is_successful is True:
            self.log.info("Dependency Check Task %s finished successfully", check_task)
        else:
            raise Exception(f"Databricks task: {check_task} failed")

    def execute(self, context: Context):
        this_run_id = self.get_run_id(context)
        api_helper = WorkflowHelper(self.mirrored_workflow, self._hook)
        self._wait_for_task_check_completion(context, this_run_id, api_helper)
        self._wait_for_task_completion(context, this_run_id, api_helper)


class DatabricksMirrorJobCreateAndStartOperator(BaseOperator):
    template_fields: Sequence[str] = ("rendered_workflow", "databricks_conn_id")

    ui_color = "#1CB1C2"
    ui_fgcolor = "#fff"
    # operator_extra_links = (DatabricksCustomLink("Mirrored Run Url", XCOM_PARENT_RUN_URL),)
    operator_extra_links = (DatabricksMirrorRunLink(),)

    def __init__(self,
                 mirrored_workflow: DatabricksMirroredWorkflow,
                 airflow_host_secret: str = None,
                 airflow_auth_header_secret: str = None,
                 *,
                 polling_notebook_path: str = None,
                 polling_notebook_dir: str = None,
                 polling_notebook_name: str = None,
                 timeout_seconds: Optional[int] = 600,
                 databricks_conn_id: str = "databricks_default", databricks_retry_limit: int = 3,
                 databricks_retry_delay: int = 1, databricks_retry_args: dict[Any, Any] | None = None,
                 do_xcom_push: bool = True, **kwargs):
        super().__init__(**kwargs, retries=0, )
        self.airflow_auth_header_secret = airflow_auth_header_secret
        self.airflow_host_secret = airflow_host_secret
        self.polling_notebook_path = polling_notebook_path
        self.polling_notebook_name = polling_notebook_name
        self.polling_notebook_dir = polling_notebook_dir
        self.mirrored_workflow = mirrored_workflow
        self.databricks_retry_args = databricks_retry_args
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_conn_id = databricks_conn_id
        self.timeout_seconds = timeout_seconds
        self.do_xcom_push = do_xcom_push
        # print("mirrored_workflow", mirrored_workflow.get_job_settings().as_dict())
        # hack to ignore jinja rendering
        self.rendered_workflow = json.loads(json.dumps(mirrored_workflow.get_job_settings().as_dict()) \
                                            .replace('"{{', '"{% raw %}{{').replace('}}"', '{% endraw %}}}"'))

    @property
    def _hook(self):
        return self._get_hook(caller="DatabricksTaskFinishedOperator")

    def _get_hook(self, caller: str) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
            retry_args=self.databricks_retry_args,
            caller=caller,
        )

    def execute(self, context: Context):
        # first make sure notebooks exist
        airflow_polling_notebook_path = self.polling_notebook_path
        if airflow_polling_notebook_path is None:
            polling_handler = PollingNotebookHandler(self._hook, self.polling_notebook_dir,
                                                     self.polling_notebook_name, self.log)
            airflow_polling_notebook_path = polling_handler.make_notebook_if_not_exists()
        self.mirrored_workflow.set_polling_notebook_path(airflow_polling_notebook_path)
        airflow_run_id = context['dag_run'].run_id
        airflow_dag_id = self.dag_id
        self.mirrored_workflow.idempotent_create_workflow_and_assets(self._hook)
        api_helper = WorkflowHelper(self.mirrored_workflow, self._hook)
        run_id = api_helper.run_now(notebook_params={
            GlobalJobParameters.DAG_ID.value: airflow_dag_id,
            GlobalJobParameters.DAG_RUN_ID.value: airflow_run_id,
            GlobalJobParameters.AIRFLOW_HOST_SECRET.value: self.airflow_host_secret,
            GlobalJobParameters.AIRFLOW_AUTH_HEADER_SECRET.value: self.airflow_auth_header_secret,
        })
        run_url = api_helper.get_task_url(run_id)
        if self.do_xcom_push and context is not None:
            context["ti"].xcom_push(key=XCOM_MIRROR_RUN_PAGE_URL_KEY, value=run_url)

        if context is not None:
            context["ti"].xcom_push(key=XCOM_PARENT_RUN_ID_KEY, value=run_id)


class AirflowDBXClusterReuseBuilder:

    def __init__(self, dag: DAG):
        self._dag = dag
        self._existing_cluster_id = None
        self._job_clusters = None
        self._cluster_key_task_mapping = None
        self._access_control_list = None
        self._job_run_as = None
        self._webhook_notifications = None
        self._timeout_seconds = None
        self._tags = None
        self._notification_settings = None
        self._email_notifications = None
        self._airflow_host_secret = None
        self._airflow_auth_header_secret = None
        self._max_concurrent_runs = None
        self._mirror_task_name = "mirror_task"

    def with_job_clusters(self, job_clusters: List[JobCluster]):
        self._job_clusters = job_clusters
        return self

    def with_existing_cluster_id(self, existing_cluster_id: str):
        self._existing_cluster_id = existing_cluster_id
        return self

    def with_cluster_key_task_mapping(self, cluster_key_task_mapping: Dict[str, str]):
        self._cluster_key_task_mapping = cluster_key_task_mapping
        return self

    def with_access_control_list(self, access_control_list: List[iam.AccessControlRequest]):
        self._access_control_list = access_control_list
        return self

    def with_job_run_as(self, job_run_as: JobRunAs):
        self._job_run_as = job_run_as
        return self

    def with_webhook_notifications(self, webhook_notifications: WebhookNotifications):
        self._webhook_notifications = webhook_notifications
        return self

    def with_timeout_seconds(self, timeout_seconds: int):
        self._timeout_seconds = timeout_seconds
        return self

    def with_tags(self, tags: Dict[str, str]):
        self._tags = tags
        return self

    def with_notification_settings(self, notification_settings: JobNotificationSettings):
        self._notification_settings = notification_settings
        return self

    def with_email_notifications(self, email_notifications: JobEmailNotifications):
        self._email_notifications = email_notifications
        return self

    def with_airflow_host_secret(self, airflow_host_secret: str):
        self._airflow_host_secret = airflow_host_secret
        return self

    def with_airflow_auth_header_secret(self, airflow_auth_header_secret: str):
        self._airflow_auth_header_secret = airflow_auth_header_secret
        return self

    def with_mirror_task_name(self, mirror_task_name: str):
        self._mirror_task_name = mirror_task_name
        return self

    def with_max_concurrent_runs(self, max_concurrent_runs: int):
        self._max_concurrent_runs = max_concurrent_runs
        return self

    def _build_databricks_mirrored_wf(self) -> DatabricksMirroredWorkflow:
        return DatabricksMirroredWorkflow(
            name=self._dag.dag_id,
            existing_cluster_id=self._existing_cluster_id,
            tasks=get_airflow_dag_to_databricks_tasks(self._dag),
            job_clusters=self._job_clusters,
            cluster_task_mapping=self._cluster_key_task_mapping,
            email_notifications=self._email_notifications,
            notification_settings=self._notification_settings,
            max_concurrent_runs=self._max_concurrent_runs,
            job_run_as=self._job_run_as,
            tags=self._tags,
            timeout_seconds=self._timeout_seconds,
            webhook_notifications=self._webhook_notifications,
            access_control_list=self._access_control_list
        )

    def build(self):
        mirrored_dag = self._build_databricks_mirrored_wf()
        orphan_tasks = mirrored_dag.orphan_databricks_tasks()
        mirror_task = DatabricksMirrorJobCreateAndStartOperator(
            task_id=self._mirror_task_name,
            mirrored_workflow=mirrored_dag,
            dag=self._dag,
            airflow_host_secret=self._airflow_host_secret,
            airflow_auth_header_secret=self._airflow_auth_header_secret
        )
        for t in orphan_tasks:
            mirror_task >> self._dag.get_task(t)
        for t in self._dag.tasks:
            if isinstance(t, DatabricksSubmitRunOperator):
                upstream = [self._dag.get_task(ut) for ut in t.upstream_task_ids]
                downstream = [self._dag.get_task(dt) for dt in t.downstream_task_ids]
                self._dag._remove_task(t.task_id)
                check_task = DatabricksTaskCheckOperator.from_submit_run_operator(self._dag,
                                                                                  t,
                                                                                  self._mirror_task_name,
                                                                                  mirrored_dag)
                for u in upstream:
                    u >> check_task
                for d in downstream:
                    check_task >> d
