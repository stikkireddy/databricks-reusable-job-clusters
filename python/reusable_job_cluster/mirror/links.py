from __future__ import annotations

import abc

from airflow.models import BaseOperatorLink, BaseOperator, XCom
from airflow.plugins_manager import AirflowPlugin

XCOM_MIRROR_RUN_PAGE_URL_KEY = "mirror_run_url"
XCOM_DEPS_CHECK_RUN_URL = "check_run_url"
XCOM_RUN_PAGE_URL_KEY = "run_page_url"


class KeyedLinkMixin(abc.ABC):

    @staticmethod
    @abc.abstractmethod
    def key():
        pass


class DatabricksMirrorRunLink(BaseOperatorLink, KeyedLinkMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Databricks Mirror Parent Job Run"

    @staticmethod
    def key():
        return XCOM_MIRROR_RUN_PAGE_URL_KEY

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_MIRROR_RUN_PAGE_URL_KEY, ti_key=ti_key)


class DatabricksDepCheckRunLink(BaseOperatorLink, KeyedLinkMixin):
    """Constructs a link to monitor a Databricks Job Run."""

    name = "Databricks Dependency Task Check Run"

    @staticmethod
    def key():
        return XCOM_DEPS_CHECK_RUN_URL

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_DEPS_CHECK_RUN_URL, ti_key=ti_key)


class DatabricksTaskRunLink(BaseOperatorLink, KeyedLinkMixin):
    """Constructs a link to monitor a Databricks Job Run. This is currently not working"""

    name = "Databricks Task Run"

    @staticmethod
    def key():
        return XCOM_DEPS_CHECK_RUN_URL

    def get_link(
            self,
            operator: BaseOperator,
            *,
            ti_key: "TaskInstanceKey",
    ) -> str:
        return XCom.get_value(key=XCOM_RUN_PAGE_URL_KEY, ti_key=ti_key)


class DatabricksMirrorJobLinks(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        DatabricksMirrorRunLink(),
        DatabricksDepCheckRunLink(),
        DatabricksTaskRunLink(),
    ]
