from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

from reusable_job_cluster.operators import DatabricksReusableJobCluster

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG object
dag = DAG('my_test_databricks_dummy_dag', default_args=default_args, schedule_interval=timedelta(days=1))

# Define the tasks/operators in the DAG
start_task = DummyOperator(task_id='start_task', dag=dag)

create_cluster_task, delete_cluster_task, existing_cluster_id = DatabricksReusableJobCluster \
    .builder() \
    .with_new_cluster({
    "autoscale": {
        "min_workers": 2,
        "max_workers": 8
    },
    "spark_version": "12.2.x-scala2.12",
    "spark_conf": {},
    "gcp_attributes": {
        "use_preemptible_executors": False,
        "availability": "ON_DEMAND_GCP",
        "zone_id": "HA"
    },
    "node_type_id": "n2-highmem-4",
    "ssh_public_keys": [],
    "custom_tags": {},
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "init_scripts": [],
    "single_user_name": "sri.tikkireddy@databricks.com",
    "data_security_mode": "SINGLE_USER",
    "runtime_engine": "STANDARD"
}) \
    .with_dag(dag) \
    .with_timeout_seconds(6000) \
    .with_task_prefix(task_prefix="reusable_cluster") \
    .build_operators()

notebook_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    databricks_conn_id="databricks_default",
    existing_cluster_id=existing_cluster_id,
    # "{{ task_instance.xcom_pull(task_ids='create_cluster_task', key='infinite_loop_cluster_id') }}",
    notebook_task={"notebook_path": "/Users/sri.tikkireddy@databricks.com/workflow-hack/helloworld"},
    dag=dag
)

dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Set up the task dependencies
start_task >> create_cluster_task >> notebook_task >> dummy_task_1 >> dummy_task_2 >> delete_cluster_task >> end_task
