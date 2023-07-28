import base64
from functools import lru_cache
from logging import Logger
from pathlib import Path

from reusable_job_cluster.vendor.hooks.databricks import DatabricksHook

POLLING_PYTHON_FILE_NAME = "airflow_polling.py"


class PollingNotebookHandler:

    def __init__(self, hook: DatabricksHook, parent_notebook_dir_path: str, parent_notebook_name: str, log: Logger):
        self._hook = hook
        self.parent_notebook_dir_path = parent_notebook_dir_path
        self.parent_notebook_name = parent_notebook_name
        self.log = log

    @lru_cache(maxsize=1)
    def _get_notebook_base64(self):
        import reusable_job_cluster.templates
        from pathlib import Path
        file = Path(reusable_job_cluster.templates.__file__).parent / POLLING_PYTHON_FILE_NAME
        with open(file, "r") as f:
            notebook_data = f.read()
        return base64.b64encode(notebook_data.encode("utf-8")).decode("utf-8")

    def _get_standard_root_dir(self):
        return str(
            Path("/Users") / Path(self._hook.get_current_user()["userName"]) / Path(".reusable_job_cluster/notebooks"))

    def _ensure_notebook_path(self):
        parent_notebook_dir_path: Path = Path(self.parent_notebook_dir_path) if self.parent_notebook_dir_path \
            else self._get_standard_root_dir()
        self._hook.do_mkdirs(str(parent_notebook_dir_path))  # ensures directory exists
        parent_notebook_name: Path = Path(self.parent_notebook_name) if self.parent_notebook_name else \
            Path(POLLING_PYTHON_FILE_NAME.rstrip(".py"))
        return str(parent_notebook_dir_path / parent_notebook_name)

    def make_notebook_if_not_exists(self):
        notebook_content = self._get_notebook_base64()
        notebook_path = self._ensure_notebook_path()
        try:
            remote_content = self._hook.do_export_notebook(notebook_path)
            if remote_content != notebook_content:
                self.log.info("Framework notebook content is different so re-uploading")
                self._hook.do_import_notebook(notebook_path, notebook_content)
            else:
                self.log.info("Framework notebook content is the same so not re-uploading")
        except Exception:
            self._hook.do_import_notebook(notebook_path, notebook_content)

        return notebook_path
