# databricks-reusable-job-clusters

## Features

1. Builder pattern for validating reusable cluster
2. Default trigger rule for destroy to ALL_DONE. Aka the cluster is destroyed if any of the previous tasks fail.
    1. You can disable this in the builder with `without_destroy_cluster_on_any_failure()`
3. Support for disabling default timeout (developers need to conciously make this choice everytime)
   1. `with_timeout_seconds(timeout_seconds=6000)` timeout parent job after 6000 seconds
   2. `with_timeout_seconds(None)` Disable timeout
   3. Either of the above options must be chosen. 
      If neither is chosen, the dag raises an error and will not be registered.
4. Support for all 3 clouds
5. Use `.build_operators(autowire=True)` to automatically add the dependencies to the DatabricksSubmitRunOperator
   1. Note there may be additional dependencies drawn to guarantee correctness 

## Best Practices

1. Use the builder to create the operators and existing cluster id
2. Use the autowire option to automatically add the dependencies to the DatabricksSubmitRunOperator
   1. Look at docker/dags/sample_dag.py for an example of no auto wiring
   2. Look at docker/dags/sample_dag_autowire.py for an example of auto wiring
3. Try to use delta cache accelerated clusters for performance and caching
4. Make sure you specify:
   1. Tags `with_tags(...)`
   2. Permissions (visibility of runs for various users) `with_view_permissions(...)` or `with_manage_permissions(...)`
   3. Timeout Seconds (cost savings) `with_timeout_seconds(...)`

## Install the package

```shell
pip install "git+https://github.com/stikkireddy/databricks-reusable-job-clusters.git#egg=databricks-reusable-job-clusters&subdirectory=python" # install a python package from a repo subdirectory
```

### Install a specific version

Install version 0.1.0

```shell
pip install "git+https://github.com/stikkireddy/databricks-reusable-job-clusters.git@0.1.0#egg=databricks-reusable-job-clusters&subdirectory=python" # install a python package from a repo subdirectory
```

## Example Dag Usage

```python
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

new_cluster = {
        "spark_version": "12.2.x-scala2.12",
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-west-2a",
            "spot_bid_price_percent": 100,
            "ebs_volume_count": 0
        },
        "node_type_id": "i3.xlarge",
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        },
        "enable_elastic_disk": False,
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 8
    }

create_cluster_task, delete_cluster_task, existing_cluster_id = DatabricksReusableJobCluster \
    .builder() \
    .with_new_cluster(new_cluster) \
    .with_dag(dag) \
    .with_timeout_seconds(6000) \
    .with_task_prefix(task_prefix="reusable_cluster") \
    .build_operators()

notebook_task = DatabricksSubmitRunOperator(
    task_id='spark_jar_task',
    databricks_conn_id="databricks_default",
    existing_cluster_id=existing_cluster_id,
    notebook_task={"notebook_path": "/Users/sri.tikkireddy@databricks.com/workflow-hack/helloworld"},
    dag=dag
)

dummy_task_1 = DummyOperator(task_id='dummy_task_1', dag=dag)
dummy_task_2 = DummyOperator(task_id='dummy_task_2', dag=dag)
end_task = DummyOperator(task_id='end_task', dag=dag)

# Set up the task dependencies
start_task >> create_cluster_task >> notebook_task >> dummy_task_1 >> dummy_task_2 >> delete_cluster_task >> end_task
```

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