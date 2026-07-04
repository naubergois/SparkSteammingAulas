# Deploy — Análise de sentimento online dos posts do X (@diariodonordeste)

Coleta posts do X do **Diário do Nordeste** e classifica o **sentimento de cada
notícia em tempo real** com **Spark Structured Streaming**.

## Arquitetura

```
producer.py (X API v2 ou exemplos) ──> ./posts_stream/*.json ──> Spark Streaming ──> sentimento por notícia (+ sentimentos.csv)
```

## Pré-requisitos

- Python 3.9+ e `spark-submit` (Apache Spark 3.5.x)
- (Opcional) Bearer Token da API do X para coletar posts reais

## Passo a passo

```bash
cd deploy_sentimento

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Coleta posts (com token = reais; sem token = exemplos)
export X_BEARER_TOKEN="SEU_TOKEN"     # opcional
python producer.py

# 2. Análise de sentimento online
spark-submit sentiment_stream_job.py
```

## Saída

- Sentimento de cada notícia impresso no console: `[POSITIVO/NEUTRO/NEGATIVO]`
- Arquivo `sentimentos.csv` acumulando os resultados

## Modelo

`nlptown/bert-base-multilingual-uncased-sentiment` (1–5 estrelas, multilíngue):
1–2 → NEGATIVO, 3 → NEUTRO, 4–5 → POSITIVO. O download do modelo ocorre na
primeira execução.
