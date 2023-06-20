# Version changelog

## 0.1.0

- Initial version release
- Support for `DatabricksCreateReusableJobClusterOperator`
- Support for `DatabricksDestroyReusableJobClusterOperator`
- Builder pattern `ReuseableJobClusterBuilder` to help create the operators and required variables
- Docker setup for testing locally with Airflow
- Example dags for all 3 clouds
- Basic documentation in README.md
- Support for auto terminating the job cluster on any dependency failure using trigger_rule
- Add autowire capability and example dag
- Added sample dags for all 3 clouds