# ── Dockerfile ──
FROM python:3.9-slim

# 1) System deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    default-libmysqlclient-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 2) Copy and install Python requirements
COPY requirements.txt /app/requirements.txt
ARG AIRFLOW_VERSION=2.7.1
ARG PYTHON_VERSION=3.9
RUN pip install --upgrade pip \
    # install Airflow with its pinned deps, including the right Pendulum
    && pip install \
    "apache-airflow==${AIRFLOW_VERSION}" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    # then install everything else
    && pip install -r /app/requirements.txt \
    && pip install -U email-validator \
    && pip install python-multipart

# 3) Create app directory and copy code
WORKDIR /app
COPY . /app

# 4) Set Airflow home & disable examples
ENV AIRFLOW_HOME=/app/airflow_home \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    MLFLOW_TRACKING_URI=http://127.0.0.1:9080

# 5) Initialize Airflow DB (so webserver/scheduler can start cleanly)
RUN airflow db init

# 6) Copy our startup script
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# 7) Expose all ports
# Airflow web UI  
EXPOSE 8080

# Streamlit dashboard  
EXPOSE 8501

# FastAPI backend  
EXPOSE 8000

# MLflow server  
EXPOSE 9080

# 8) Entrypoint kicks off everything
ENTRYPOINT ["/app/entrypoint.sh"]

