# Version changelog

## 0.2.0

- Remove old features and switch to mirroring strategy
  - Airflow dags will be mirrored in databricks and will be kept in sync
  - Databricks tasks will monitor airflow to see if dependencies are met
