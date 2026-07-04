# Deploy — Treino incremental de Diabetes (Spark Streaming + MinIO/S3)

Treina um classificador de diabetes de forma incremental conforme os pacientes
chegam por um fluxo, salvando cada checkpoint (modelo, métricas e a superfície de
separação) no **MinIO** — o S3 open source da AWS.

## Arquitetura

```
producer.py ──> ./diabetes_stream/*.json ──> Spark Streaming (partial_fit) ──> MinIO (S3: models/ metrics/ plots/)
```

## Pré-requisitos

- Docker + Docker Compose
- Python 3.9+ e `spark-submit` (Apache Spark 3.5.x)

## Passo a passo

```bash
cd deploy_diabetes

# 1. Sobe o MinIO (S3) e cria o bucket 'diabetes-training'
docker compose up -d

# 2. Ambiente Python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Gera o stream de pacientes (em outro terminal ou antes do job)
python producer.py

# 4. Roda o job de treino incremental (salva no MinIO/S3)
spark-submit stream_train_job.py
```

## Endpoints

| Serviço          | Endereço                          |
|------------------|-----------------------------------|
| MinIO (API S3)   | http://localhost:9000             |
| MinIO (console)  | http://localhost:9001 (`minioadmin`/`minioadmin`) |
| Bucket           | `diabetes-training`               |

## Inspecionar os artefatos salvos

No console web (http://localhost:9001) ou via `mc`:

```bash
docker run --rm --network host minio/mc sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin &&
  mc ls -r local/diabetes-training"
```

## Encerrar

```bash
docker compose down        # mantém os dados
docker compose down -v      # remove o MinIO e os artefatos
```
