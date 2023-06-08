# databricks-reusable-job-clusters


# Docker instructions

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