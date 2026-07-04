"""Spark Structured Streaming: análise de sentimento online dos posts do X.

Lê os posts de STREAM_DIR (1 arquivo = 1 micro-batch) e classifica o sentimento
de cada notícia em PT-BR, imprimindo o resultado e salvando um CSV acumulado.

Executar:
    spark-submit sentiment_stream_job.py
"""
import os

import pandas as pd
from transformers import pipeline

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

STREAM_DIR = os.getenv("STREAM_DIR", "./posts_stream")
OUT_CSV = os.getenv("OUT_CSV", "./sentimentos.csv")

sentimento = pipeline(
    "sentiment-analysis",
    model="nlptown/bert-base-multilingual-uncased-sentiment",
    truncation=True,
)


def classificar(texto: str):
    res = sentimento(texto[:512])[0]
    estrelas = int(res["label"][0])
    if estrelas <= 2:
        rotulo = "NEGATIVO"
    elif estrelas == 3:
        rotulo = "NEUTRO"
    else:
        rotulo = "POSITIVO"
    return rotulo, estrelas, round(float(res["score"]), 3)


def analisar_batch(batch_df, batch_id: int) -> None:
    linhas = batch_df.collect()
    if not linhas:
        return
    registros = []
    for r in linhas:
        rotulo, estrelas, score = classificar(r["texto"])
        registros.append({"id": r["id"], "sentimento": rotulo,
                          "estrelas": estrelas, "confianca": score, "texto": r["texto"]})
        print(f"[{rotulo}] ({score:.0%}) {r['texto'][:80]}")
    df = pd.DataFrame(registros)
    header = not os.path.exists(OUT_CSV)
    df.to_csv(OUT_CSV, mode="a", header=header, index=False)


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("SentimentoOnlineDiarioNordeste")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("texto", StringType(), False),
        StructField("criado_em", StringType(), True),
    ])
    df = (
        spark.readStream.schema(schema)
        .option("maxFilesPerTrigger", 1)
        .json(STREAM_DIR)
    )
    query = (
        df.writeStream.outputMode("append")
        .foreachBatch(analisar_batch)
        .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/checkpoint_sentimento"))
        .trigger(processingTime="2 seconds")
        .start()
    )
    print(f"Análise de sentimento iniciada (query id: {query.id}). Ctrl+C para encerrar.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
