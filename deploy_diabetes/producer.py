"""Produtor do stream de pacientes (Pima Indians Diabetes).

Grava lotes de pacientes (JSON Lines) numa pasta monitorada pelo job Spark,
simulando a chegada incremental de exames.

Uso:
    python producer.py
Variáveis de ambiente (opcionais):
    STREAM_DIR (default ./diabetes_stream)
    CHUNK      (default 30)
    INTERVALO  (default 3 segundos)
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

STREAM_DIR = Path(os.getenv("STREAM_DIR", "./diabetes_stream"))
CHUNK = int(os.getenv("CHUNK", "30"))
INTERVALO = float(os.getenv("INTERVALO", "3"))

URL = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/pima-indians-diabetes.data.csv"
COLS = ["preg", "glucose", "bp", "skin", "insulin", "bmi", "pedigree", "age", "outcome"]


def main() -> None:
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    dados = pd.read_csv(URL, header=None, names=COLS)
    dados = dados[(dados["glucose"] > 0) & (dados["bmi"] > 0)].reset_index(drop=True)

    X = dados[["glucose", "bmi"]].values.astype(float)
    y = dados["outcome"].values.astype(int)
    X_train, _, y_train, _ = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    rng = np.random.default_rng(7)
    ordem = rng.permutation(len(X_train))

    lote = 0
    for ini in range(0, len(ordem), CHUNK):
        lote += 1
        idxs = ordem[ini:ini + CHUNK]
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        linhas = [
            json.dumps({
                "glucose": float(X_train[i, 0]),
                "bmi": float(X_train[i, 1]),
                "outcome": int(y_train[i]),
            })
            for i in idxs
        ]
        arq = STREAM_DIR / f"lote{lote:03d}_{ts}.json"
        arq.write_text("\n".join(linhas), encoding="utf-8")
        print(f"lote {lote}: {len(idxs)} pacientes -> {arq}")
        time.sleep(INTERVALO)
    print("Produtor encerrado.")


if __name__ == "__main__":
    main()
