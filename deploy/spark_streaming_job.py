"""Job Spark Structured Streaming: Kafka (votos) -> agregação por estado -> MongoDB.

Executar:
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      spark_streaming_job.py

Variáveis de ambiente (opcionais):
    KAFKA_BOOTSTRAP (default localhost:9092)
    KAFKA_TOPIC     (default votos)
    MONGO_URI       (default mongodb://localhost:27017)
"""
import os
from datetime import datetime

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "votos")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "eleicao")
MONGO_COLL = os.getenv("MONGO_COLL", "apuracao")

schema = StructType([
    StructField("id_urna", IntegerType(), True),
    StructField("estado", StringType(), False),
    StructField("candidato", StringType(), False),
    StructField("event_time", StringType(), True),
])


def grava_mongo(batch_df, batch_id: int) -> None:
    linhas = batch_df.collect()
    if not linhas:
        return
    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLL]
    for r in linhas:
        _id = f"{r['estado']}|{r['candidato']}"
        coll.update_one(
            {"_id": _id},
            {"$set": {
                "estado": r["estado"],
                "candidato": r["candidato"],
                "votos": int(r["count"]),
                "atualizado_em": datetime.utcnow(),
            }},
            upsert=True,
        )
    client.close()
    print(f"Batch {batch_id}: {len(linhas)} par(es) (estado, candidato) atualizados no MongoDB.")


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("EleicaoHomerSpockKafkaMongo")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    votos_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    votos_df = (
        votos_raw
        .select(from_json(col("value").cast("string"), schema).alias("v"))
        .select("v.*")
    )

    apuracao_df = votos_df.groupBy("estado", "candidato").count()

    query = (
        apuracao_df.writeStream
        .outputMode("update")
        .foreachBatch(grava_mongo)
        .option("checkpointLocation", os.getenv("CHECKPOINT_DIR", "/tmp/checkpoint_eleicao"))
        .trigger(processingTime="8 seconds")
        .start()
    )

    print(f"Streaming iniciado (query id: {query.id}). Ctrl+C para encerrar.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
