FROM apache/airflow:2.4.1
RUN whoami
COPY docker/requirements.txt .
RUN pip install -r requirements.txt
RUN mkdir -p /tmp/reuse-code/python/
COPY python /tmp/reuse-code/python/
RUN cd /tmp/reuse-code/python/ && pip install .