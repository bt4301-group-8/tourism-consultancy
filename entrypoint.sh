#!/usr/bin/env bash
set -e

echo "📁 Current working directory:"
pwd

# Optional: list contents
echo "📄 Files in this directory:"
ls -la

# initialize Airflow metadata DB if not already done
airflow db init

# 2) Kick off Airflow
airflow scheduler &
echo "✔️ Airflow scheduler started"
airflow webserver --port 8080 &
echo "✔️ Airflow webserver on :8080"

# 1) Start MLflow Tracking Server (artifacts and backend store in /app/mlflow_artifacts)
mlflow server --host 0.0.0.0 --port 9080 &
echo "✔️ MLflow on :9080"
python -m backend.src.ml_pipeline

# 3) Launch Streamlit frontend
streamlit run /app/frontend/src/app.py --server.port 8501 &
echo "✔️ Streamlit on :8501"

# 4) Finally, run FastAPI (blocks here)
uvicorn backend.src.main:app --host 0.0.0.0 --port 8000
