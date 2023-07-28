# Databricks notebook source
import functools
import json
import logging
import time
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from dataclasses import dataclass
from typing import List

from enum import Enum


# TODO: potentially install this as a library in notebook but for now this is copied from mirror/__init__.py
# TODO: replace print with logging

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


@dataclass
class TasksStateInput:
    dag_id: str
    dag_run_id: str
    task_id: str


@dataclass
class AirflowDeps:
    upstream_task_inputs: List[TasksStateInput]
    this_task: TasksStateInput
    trigger_rule: str

    @classmethod
    def from_fields(cls, dag_id, dag_run_id, task_id, upstream_task_ids, trigger_rule):
        upstream_tasks = []
        for upstream_task_id in upstream_task_ids:
            upstream_tasks.append(
                TasksStateInput(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=upstream_task_id,
                )
            )
        this_task = TasksStateInput(
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
        )
        return cls(
            upstream_task_inputs=upstream_tasks,
            this_task=this_task,
            trigger_rule=trigger_rule
        )

    @classmethod
    def from_job_params(cls):
        _dbutils = globals()['dbutils']
        dag_id = _dbutils.widgets.get(GlobalJobParameters.DAG_ID.value)
        dag_run_id = _dbutils.widgets.get(GlobalJobParameters.DAG_RUN_ID.value)
        task_id = _dbutils.widgets.get(TaskSpecificParameters.TASK_ID.value)
        upstream_task_ids = json.loads(_dbutils.widgets.get(TaskSpecificParameters.UPSTREAM_TASK_IDS.value))
        trigger_rule = _dbutils.widgets.get(TaskSpecificParameters.TRIGGER_RULE.value)
        return cls.from_fields(dag_id, dag_run_id, task_id, upstream_task_ids, trigger_rule)


class Outcome(Enum):
    NONE = "NONE"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    SKIPPED_DUE_TO_AIRFLOW_SKIP = "SKIPPED_DUE_TO_AIRFLOW_SKIP"
    SKIPPED_DUE_TO_AIRFLOW_SUCCESS = "SKIPPED_DUE_TO_AIRFLOW_SUCCESS"
    FAILED_DUE_TO_AIRFLOW_FAILURE = "FAILED_DUE_TO_AIRFLOW_FAILURE"
    FAILED_DUE_TO_AIRFLOW_UPSTREAM = "FAILED_DUE_TO_AIRFLOW_UPSTREAM"


class PollingState(Enum):
    POLLING = "POLLING"
    FINISHED = "FINISHED"


class TaskFailedInAirflowException(Exception):
    pass


class StateManager:

    def __init__(self,
                 airflow_deps: AirflowDeps = None,
                 airflow_polling_interval: int = 10):
        self._airflow_polling_interval = airflow_polling_interval
        self._airflow_deps = airflow_deps or AirflowDeps.from_job_params()
        # refer to: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#task-instances
        self._task_finished_states = [
            "success",
            "shutdown",
            "failed",
            "skipped",
            "upstream_failed",
            "removed",
        ]
        self._task_success_states = ["success"]
        self._task_failed_states = ["failed", "upstream_failed"]
        self._task_skipped_states = ["skipped"]
        self._task_active_states = [
            "none",
            "scheduled",
            "queued",
            "running",
            "restarting",
            "up_for_retry",
            "up_for_reschedule",
            "deferred",
        ]

    @property
    def dbutils(self):
        return globals()['dbutils']

    def parse_url_scheme(self, input_str: str):
        parsed_url = urlparse(input_str)
        if parsed_url.scheme == "secrets":
            print(f"[INFO] Resolving secret from secret scope: {input_str}")
            auth_header = self.dbutils.secrets.get(scope=parsed_url.netloc, key=parsed_url.path.lstrip("/"))
            if auth_header is None or auth_header == "":
                raise ValueError(f"Could not resolve secret from secret scope: {input_str}")
            return auth_header
        else:
            return input_str

    def _get_airflow_host(self):
        raw_host = self.dbutils.widgets.get(GlobalJobParameters.AIRFLOW_HOST_SECRET.value).rstrip("/")
        try:
            host = self.parse_url_scheme(raw_host)
        except Exception:
            print("Failed to parse url scheme for host secret, falling back to plain text host")
            host = raw_host
        if host.endswith("/api/v1"):
            return host + "/"
        else:
            return f"{host}/api/v1/"

    def _get_url(self, path: str) -> str:
        return self._get_airflow_host() + path.lstrip("/")

    def _get_airflow_auth_header(self):
        header_info = self.dbutils.widgets.get(GlobalJobParameters.AIRFLOW_AUTH_HEADER_SECRET.value)
        try:
            return self.parse_url_scheme(header_info)
        except Exception:
            print("Failed to parse url scheme for header secret, falling back to plain text")
            return self.dbutils.widgets.get(GlobalJobParameters.AIRFLOW_AUTH_HEADER_SECRET.value)

    def get_task_state(self, task: TasksStateInput) -> str:
        return self.get_task_state_requests(task)

    @functools.lru_cache(maxsize=1)
    def get_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update({'Authorization': self._get_airflow_auth_header()})

        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        logging.basicConfig(level=logging.INFO)  # Set up logging to INFO level

        return session

    def validate_airflow_connection(self):
        session = self.get_session()
        health_resp = session.get(self._get_url("health"))
        if health_resp.status_code != 200:
            raise ValueError(f"Failed to connect to airflow to /health: {health_resp.text}")
        else:
            scheduler_health = health_resp.json().get("scheduler", {}).get("status", "Unknown")
            print(f"[INFO] Connection to airflow /health successful. Scheduler health: {scheduler_health}")
        version_resp = session.get(self._get_url("version"))
        if version_resp.status_code != 200:
            raise ValueError(f"Failed to connect to airflow to /version: {version_resp.text}")
        else:
            print(f"[INFO] Connection to airflow /version successful: {version_resp.json()['version']}")

    def get_task_state_requests(self, task: TasksStateInput):
        session = self.get_session()
        path = f"dags/{task.dag_id}/" \
               f"dagRuns/{task.dag_run_id}/" \
               f"taskInstances/{task.task_id}"

        response = session.request("GET", self._get_url(path), data={})
        task_state = response.json().get("state")
        print(f"[INFO] Fetching task state from airflow for "
              f"task: {task.task_id} "
              f"dag: {task.dag_id} "
              f"run_id: {task.dag_run_id} "
              f"state: {task_state}")
        return task_state

    def determine_deps_completed(self, states: List[str]):
        # handling only trigger all success
        return all([state in self._task_finished_states for state in states])

    def determine_deps_success(self, states: List[str]):
        # handling only trigger all success
        return all([state in self._task_success_states for state in states])

    def set_outcome(self, outcome: Outcome):
        self.dbutils.jobs.taskValues.set(key=PollerTaskValues.RESULT_STATE_KEY.value, value=outcome.value)

    def wait_for_outcome(self):
        print(f"[INFO] Determining outcome for "
              f"task: {self._airflow_deps.this_task.task_id} "
              f"dag: {self._airflow_deps.this_task.dag_id} "
              f"run_id: {self._airflow_deps.this_task.dag_run_id}"
              f"trigger_rule: {self._airflow_deps.trigger_rule} "
              f"upstream_tasks: {[task.task_id for task in self._airflow_deps.upstream_task_inputs]}")

        self.validate_airflow_connection()

        state, outcome = self._determine_outcome()
        while state == PollingState.POLLING:
            state, outcome = self._determine_outcome()
            time.sleep(self._airflow_polling_interval)
        self.set_outcome(outcome)
        if outcome in [Outcome.FAILED_DUE_TO_AIRFLOW_FAILURE, Outcome.FAILED_DUE_TO_AIRFLOW_UPSTREAM]:
            raise TaskFailedInAirflowException(f"Task {self._airflow_deps.this_task.task_id} "
                                               f"failed in airflow due to: {outcome.value}")
        print(f"[INFO] Polling upstream dependencies: "
              f"{[task.task_id for task in self._airflow_deps.upstream_task_inputs]} finished for "
              f"task: {self._airflow_deps.this_task.task_id} with outcome: ", outcome)
        return state, outcome

    def _determine_outcome(self):
        # incase this task is already done in airflow
        this_task_state = self.get_task_state(self._airflow_deps.this_task)
        outcomes = [
            (self._task_success_states, PollingState.FINISHED, Outcome.SKIPPED_DUE_TO_AIRFLOW_SUCCESS),
            (self._task_skipped_states, PollingState.FINISHED, Outcome.SKIPPED_DUE_TO_AIRFLOW_SKIP),
            (self._task_failed_states, PollingState.FINISHED, Outcome.FAILED_DUE_TO_AIRFLOW_FAILURE),
        ]
        for states, polling_state, outcome in outcomes:
            if this_task_state in states:
                print(f"current task: {self._airflow_deps.this_task.task_id} is already done in airflow; "
                      f"state: {polling_state}; "
                      f"outcome: {outcome}")
                return polling_state, outcome

        # assuming this task is not done lets go to mainstream
        deps = self._airflow_deps.upstream_task_inputs
        states = [self.get_task_state(task) for task in deps]
        are_deps_met = self.determine_deps_completed(states)
        if are_deps_met is False:
            return PollingState.POLLING, Outcome.NONE
        are_deps_successful = self.determine_deps_success(states)

        if are_deps_successful is False:
            return PollingState.FINISHED, Outcome.FAILED_DUE_TO_AIRFLOW_UPSTREAM

        # if upstream are successful, and this task is not done yet
        return PollingState.FINISHED, Outcome.SUCCESS


# COMMAND ----------

sm = StateManager(
    airflow_deps=AirflowDeps.from_job_params()
)
this_state, this_outcome = sm.wait_for_outcome()
sm.set_outcome(this_outcome)
