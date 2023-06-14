FROM apache/airflow:2.4.1
ENV SETUPTOOLS_SCM_PRETEND_VERSION=1.0.0
RUN whoami
COPY docker/requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir -p /tmp/reuse-code/python/
COPY python /tmp/reuse-code/python/
COPY .git /tmp/reuse-code/.git
RUN cd /tmp/reuse-code/python/ && pip install .