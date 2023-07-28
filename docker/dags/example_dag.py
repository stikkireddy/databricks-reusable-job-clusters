from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from databricks.sdk.service import compute
from databricks.sdk.service.jobs import JobCluster

from reusable_job_cluster.mirror.operators import AirflowDBXClusterReuseBuilder

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG object
dag = DAG('test_dbx_aws_dag_reuse',
          default_args=default_args,
          schedule_interval=None
          )

# Define the tasks/operators in the DAG
start_task = DummyOperator(task_id='start_task', dag=dag)

notebook_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    databricks_conn_id="databricks_default",
    existing_cluster_id="existing_cluster_id",
    # "{{ task_instance.xcom_pull(task_ids='create_cluster_task', key='infinite_loop_cluster_id') }}",
    notebook_task={"notebook_path": "/Users/sri.tikkireddy@databricks.com/workflow-mirroring/helloworld"},
    dag=dag
)

notebook_task_2 = DatabricksSubmitRunOperator(
    task_id='spark_jar_task_2',
    databricks_conn_id="databricks_default",
    existing_cluster_id="existing_cluster_id",
    # "{{ task_instance.xcom_pull(task_ids='create_cluster_task', key='infinite_loop_cluster_id') }}",
    notebook_task={"notebook_path": "/Users/sri.tikkireddy@databricks.com/workflow-mirroring/helloworld"},
    dag=dag
)

dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
dummy_task_3 = DummyOperator(task_id='dummy_task_3', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)


def branch_func(**kwargs):
    return "dummy_task_3"


branch_op = BranchPythonOperator(
    task_id='branch_task',
    provide_context=True,
    python_callable=branch_func,
    dag=dag)

start_task >> notebook_task >> dummy_task_1 >> branch_op
branch_op >> [dummy_task_3, notebook_task_2]
notebook_task_2 >> dummy_task_2 >> end_task

(AirflowDBXClusterReuseBuilder(dag)
 .with_job_clusters([JobCluster(
    new_cluster=compute.ClusterSpec(
        driver_node_type_id="n2-highmem-4",
        node_type_id="n2-highmem-4",
        num_workers=2,
        spark_version="12.2.x-scala2.12",
        spark_conf={"spark.databricks.delta.preview.enabled": "true"},
    ),
    job_cluster_key="job_cluster"
)])
 .with_airflow_host_secret("secrets://sri-scope-2/airflow_host")
 .with_airflow_auth_header_secret("secrets://sri-scope-2/airflow_header")
 .build())
