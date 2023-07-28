from enum import Enum


class GlobalJobParameters(Enum):
    DAG_ID = "dag_id"
    DAG_RUN_ID = "dag_run_id"
    AIRFLOW_HOST_SECRET = "airflow_host_secret"
    AIRFLOW_AUTH_HEADER_SECRET = "airflow_auth_header_secret"


class TaskSpecificParameters(Enum):
    TASK_ID = "task_id"
    UPSTREAM_TASK_IDS = "upstream_task_ids"
    TRIGGER_RULE = "trigger_rule"


class PollerTaskValues(Enum):
    RESULT_STATE_KEY = "result_state"
