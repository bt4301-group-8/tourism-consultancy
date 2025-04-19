from fastapi import FastAPI, UploadFile, File
import mlflow, os

app = FastAPI()
mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI"))

@app.post("/models/save")
async def save_model(
    model_file: UploadFile = File(...),
    model_name: str = "default_model",
    stage: str = "None"
):
    tmp = f"/tmp/{model_file.filename}"
    with open(tmp, "wb") as f:
        f.write(await model_file.read())

    with mlflow.start_run():
        mlflow.log_artifact(tmp, artifact_path="model")
        mlflow.register_model(
            f"runs:/{mlflow.active_run().info.run_id}/model/{model_file.filename}",
            name=model_name
        )
    os.remove(tmp)
    return {"status": "saved", "model": model_name, "stage": stage}
