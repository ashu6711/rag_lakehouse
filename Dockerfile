FROM apache/airflow:2.10.0-python3.12

ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/jobs"
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow/helper"

USER root

RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

COPY ./setup.py ./setup.py
RUN pip install --upgrade pip && pip install --no-cache-dir --compile --editable .

# RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"



