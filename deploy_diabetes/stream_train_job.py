"""Spark Structured Streaming: treino incremental de diabetes salvando no MinIO (S3).

Lê o stream de pacientes de STREAM_DIR, treina um SGDClassifier com partial_fit a
cada micro-batch e salva modelo (.pkl), métricas (.json) e a superfície de
separação (.png) no MinIO/S3.

Executar:
    spark-submit stream_train_job.py

Variáveis de ambiente (opcionais):
    STREAM_DIR   (default ./diabetes_stream)
    S3_ENDPOINT  (default http://localhost:9000)
    S3_BUCKET    (default diabetes-training)
"""
import io
import json
import os
import pickle
from datetime import datetime

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

STREAM_DIR = os.getenv("STREAM_DIR", "./diabetes_stream")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_KEY = os.getenv("S3_KEY", "minioadmin")
S3_SECRET = os.getenv("S3_SECRET", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "diabetes-training")

URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"
COLS = ["preg", "glucose", "bp", "skin", "insulin", "bmi", "pedigree", "age", "outcome"]

# --- Conjunto de teste fixo + scaler (para medir a acurácia de forma justa) ---
_d = pd.read_csv(URL, header=None, names=COLS)
_d = _d[(_d["glucose"] > 0) & (_d["bmi"] > 0)].reset_index(drop=True)
_X = _d[["glucose", "bmi"]].values.astype(float)
_y = _d["outcome"].values.astype(int)
X_train, X_test, y_train, y_test = train_test_split(
    _X, _y, test_size=0.25, random_state=42, stratify=_y
)
scaler = StandardScaler().fit(X_train)
X_test_s = scaler.transform(X_test)

s3 = boto3.client(
    "s3", endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET,
    region_name="us-east-1",
)
try:
    s3.create_bucket(Bucket=S3_BUCKET)
except Exception:
    pass

model = SGDClassifier(loss="log_loss", alpha=1e-3, random_state=42)
_estado = {"init": False, "vistos": 0}


def _plot_png(batch_id: int, acc: float) -> bytes:
    x_min, x_max = X_test_s[:, 0].min() - 1, X_test_s[:, 0].max() + 1
    y_min, y_max = X_test_s[:, 1].min() - 1, X_test_s[:, 1].max() + 1
    xx, yy = np.meshgrid(np.linspace(x_min, x_max, 200),
                         np.linspace(y_min, y_max, 200))
    Z = model.predict(np.c_[xx.ravel(), yy.ravel()]).reshape(xx.shape)
    fig, ax = plt.subplots(figsize=(6.5, 5))
    ax.contourf(xx, yy, Z, alpha=0.25, cmap="coolwarm", levels=1)
    ax.contour(xx, yy, Z, colors="k", linewidths=1, levels=[0.5])
    ax.scatter(X_test_s[y_test == 0, 0], X_test_s[y_test == 0, 1], c="#2b6cb0", s=18, edgecolor="w")
    ax.scatter(X_test_s[y_test == 1, 0], X_test_s[y_test == 1, 1], c="#c53030", s=18, edgecolor="w")
    ax.set_title(f"Batch {batch_id} | vistos={_estado['vistos']} | acc={acc:.1%}")
    ax.set_xlabel("Glicose (padronizada)")
    ax.set_ylabel("IMC/BMI (padronizado)")
    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=90)
    plt.close(fig)
    return buf.getvalue()


def treinar_batch(batch_df, batch_id: int) -> None:
    pdf = batch_df.toPandas()
    if pdf.empty:
        return
    Xb = scaler.transform(pdf[["glucose", "bmi"]].values)
    yb = pdf["outcome"].values.astype(int)
    if not _estado["init"]:
        model.partial_fit(Xb, yb, classes=np.array([0, 1]))
        _estado["init"] = True
    else:
        model.partial_fit(Xb, yb)
    _estado["vistos"] += len(yb)
    acc = accuracy_score(y_test, model.predict(X_test_s))

    s3.put_object(Bucket=S3_BUCKET, Key=f"models/model_batch{batch_id:03d}.pkl",
                  Body=pickle.dumps({"model": model, "scaler": scaler}))
    s3.put_object(Bucket=S3_BUCKET, Key=f"metrics/metrics_batch{batch_id:03d}.json",
                  Body=json.dumps({"batch": batch_id, "pacientes_vistos": _estado["vistos"],
                                   "acuracia": acc, "ts": datetime.utcnow().isoformat()}).encode())
    s3.put_object(Bucket=S3_BUCKET, Key=f"plots/boundary_batch{batch_id:03d}.png",
                  Body=_plot_png(batch_id, acc), ContentType="image/png")
    print(f"Batch {batch_id}: acuracia={acc:.1%} | +{len(yb)} pacientes | salvo no S3")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("DiabetesStreamingTrainMinIO")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("glucose", DoubleType(), False),
        StructField("bmi", DoubleType(), False),
        StructField("outcome", IntegerType(), False),
    ])
    df = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(STREAM_DIR)
    )
    query = (
        df.writeStream.outputMode("append")
        .foreachBatch(treinar_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/checkpoint_diabetes"))
        .trigger(processingTime="4 seconds")
        .start()
    )
    print(f"Treino em streaming iniciado (query id: {query.id}). Ctrl+C para encerrar.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
