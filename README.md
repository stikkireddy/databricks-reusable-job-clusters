# databricks-reusable-job-clusters

## Features

1. Builder pattern for supporting cluster reuse. 
   1. **Currently it only supports one cluster per airflow dag!**
2. ** Currently only processes trigger rule for all success **
3. Support for all 3 clouds (AWS, Azure, GCP)
4. TODO: require default timeout_seconds for job runs, and default tags

## Best Practices

1. Use the builder to create the operators and existing cluster id
2. Try to use delta cache accelerated clusters for performance and caching
3. Once you have it running with existing cluster id then configure a new jobs cluster.


## Install the latest version of the package

```shell
pip install "git+https://github.com/stikkireddy/databricks-reusable-job-clusters.git#egg=databricks-reusable-job-clusters&subdirectory=python" # install a python package from a repo subdirectory
```

#### Read [changelog]((CHANGELOG.md)) for more details.

### Install a specific version of the package

Install version 0.2.0

```shell
pip install "git+https://github.com/stikkireddy/databricks-reusable-job-clusters.git@0.2.0#egg=databricks-reusable-job-clusters&subdirectory=python" # install a python package from a repo subdirectory
```

## Example Dag Usage

```python
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
    notebook_task={"notebook_path": "/Users/sri.tikkireddy@databricks.com/workflow-mirroring/helloworld"},
    dag=dag
)

notebook_task_2 = DatabricksSubmitRunOperator(
    task_id='spark_jar_task_2',
    databricks_conn_id="databricks_default",
    existing_cluster_id="existing_cluster_id",
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
        driver_node_type_id="i3.xlarge", # or a different vm type depending on the cloud
        node_type_id="i3.xlarge",  # or a different vm type depending on the cloud
        num_workers=2,
        spark_version="12.2.x-scala2.12",
        spark_conf={"spark.databricks.delta.preview.enabled": "true"},
    ),
    job_cluster_key="job_cluster"
  )])
  # .with_existing_cluster_id("existing_cluster_id") # use this if you want to use an existing cluster
 .with_airflow_host_secret("secrets://sri-scope-2/airflow_host")
 .with_airflow_auth_header_secret("secrets://sri-scope-2/airflow_header")
 .build())
```

## SOP for rerunning portion of an existing dag run.

1. Pause the dag
2. Go to the run that you wish to clear portions of.
3. Clear the mirror task (disable downstream and recursive).
4. Clear any additional tasks that you need to clear.
5. Resume the dag


## Docker instructions

1. Run this first to setup airflow user

```shell
cd docker
mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

2. Run the init command

```shell
docker compose up airflow-init
```

3. Run the airflow webserver

```shell
docker compose up --build
```

4. Open airflow http://localhost:8088 and login with username: `airflow` and password: `airflow`


5. Add the databricks connection configuration in the admin console

## Disclaimer
databricks-reusable-job-clusters is not developed, endorsed not supported by Databricks. 
It is provided as-is; no warranty is derived from using this package. For more details, please refer to the license.