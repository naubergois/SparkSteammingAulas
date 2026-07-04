# Deploy — Eleição Homer x Spock (Kafka + Spark Streaming + MongoDB)

Pipeline de apuração dinâmica de votos por estado do Brasil, pronto para rodar
fora do Colab.

## Arquitetura

```
producer.py ──> Kafka (tópico "votos") ──> Spark Structured Streaming ──> MongoDB (db "eleicao")
```

## Pré-requisitos

- Docker + Docker Compose
- Python 3.9+ e `spark-submit` (Apache Spark 3.5.x) para o job de streaming

## Passo a passo

```bash
# 1. Sobe Kafka + MongoDB (+ Mongo Express)
cd deploy
docker compose up -d

# 2. Cria o ambiente Python e instala dependências
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Roda o job Spark (consumidor Kafka -> MongoDB)
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_streaming_job.py

# 4. Em outro terminal, roda o produtor de votos
source .venv/bin/activate
python producer.py
```

## Endpoints

| Serviço        | Endereço                  |
|----------------|---------------------------|
| Kafka          | `localhost:9092`          |
| MongoDB        | `localhost:27017`         |
| Mongo Express  | http://localhost:8081     |

## Consultar a apuração

```bash
docker exec -it eleicao-mongo mongosh eleicao --eval '
  db.apuracao.aggregate([
    { $group: { _id: "$candidato", votos: { $sum: "$votos" } } },
    { $sort: { votos: -1 } }
  ]).toArray()
'
```

## Encerrar

```bash
docker compose down          # mantém os volumes
docker compose down -v       # remove Kafka + MongoDB e os dados
```
