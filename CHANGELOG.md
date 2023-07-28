# Version changelog

## 0.2.0

- Remove old features and switch to mirroring strategy
  - Airflow dags will be mirrored in databricks and will be kept in sync
  - Databricks tasks will monitor airflow to see if dependencies are met
- Added handy links in the task operators to the databricks UI as plugin in airflow
- Updated README.md with new instructions
- Detailed logging on both the airflow side and polling side on databricks.
