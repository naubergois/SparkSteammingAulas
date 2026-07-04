# Spark Streaming — Exercícios (Colab)

## Exercício 1 — Cotações de Bolsa → MySQL

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_bolsa_mysql.ipynb)

Exercício prático para Google Colab: simula um fluxo contínuo de cotações de ações (B3) com **Apache Spark Structured Streaming** e grava os dados em **MySQL instalado localmente no Colab**.

## Exercício 2 — Voos de Companhias Aéreas + Watermark → PostgreSQL

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_voos_postgresql.ipynb)

Simula um fluxo contínuo de eventos de voos e usa **watermark** com **janelas temporais** para calcular a **evolução dos voos ao longo do tempo** por companhia, gravando as agregações em **PostgreSQL instalado localmente no Colab**.

**Abrir direto:** https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_voos_postgresql.ipynb

## Exercício 3 — Eleição Homer x Spock (Kafka + Spark Streaming → MongoDB)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_eleicao_mongodb.ipynb)

Simula uma eleição entre **Homer Simpson** e **Spock** com **apuração dinâmica de votos por estado (UF)**. As urnas publicam votos no **Apache Kafka**, o **Spark Structured Streaming** agrega os totais por estado/candidato e grava a apuração no **MongoDB**, mostrando quem vence em cada estado e no voto popular.

**Abrir direto:** https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_eleicao_mongodb.ipynb

**Deploy fora do Colab (Docker Compose):** veja [`deploy/`](deploy/README.md) — sobe Kafka + MongoDB e roda o produtor + o job Spark via `spark-submit`.

## Exercício 4 — Treino incremental de ML (Diabetes) + Spark Streaming → MinIO (S3)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_diabetes_minio.ipynb)

Treina um classificador de **diabetes** de forma **incremental (online learning)** conforme os pacientes chegam por um fluxo (**Spark Structured Streaming**), salvando cada checkpoint do modelo no **MinIO** — o **S3 open source da AWS**. A cada micro-batch mostra a **acurácia** e a **superfície de separação** (fronteira de decisão) evoluindo ao longo do treino.

**Abrir direto:** https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_diabetes_minio.ipynb

**Deploy fora do Colab (Docker Compose):** veja [`deploy_diabetes/`](deploy_diabetes/README.md) — sobe o MinIO (S3) e roda o produtor + o job de treino via `spark-submit`.

## Abrir no Colab Research

Clique no badge acima ou acesse diretamente:

**https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_bolsa_mysql.ipynb**

## O que você vai aprender

- Instalar e configurar **MySQL Server** dentro do Colab
- Buscar cotações em tempo quasi-real com `yfinance`
- Simular um stream de dados (pasta monitorada pelo Spark)
- Processar micro-batches com Structured Streaming
- Persistir no MySQL via JDBC (`foreachBatch`)

## Pré-requisitos

1. Conta no [Google Colab](https://colab.research.google.com/)
2. Executar o notebook — **MySQL é instalado automaticamente na VM do Colab**

## MySQL local no Colab

O notebook instala e configura automaticamente:

| Parâmetro | Valor |
|-----------|-------|
| Host | `127.0.0.1` |
| Porta | `3306` |
| Banco | `bolsa` |
| Usuário | `spark` |
| Senha | `spark123` |

> Os dados ficam na VM temporária do Colab. Ao encerrar a sessão, o MySQL e os dados são apagados.

## Como usar no Colab

1. Clique no badge **Open in Colab** (ou no link acima)
2. **Runtime → Factory reset runtime** (importante se já abriu uma versão antiga)
3. **Runtime → Run all**
3. Aguarde a instalação do MySQL (~1–2 min na primeira célula)
4. O **produtor** roda em background por ~2 minutos gerando arquivos JSON
5. O **Spark Streaming** consome os arquivos e grava no MySQL local
6. Valide com a célula de consulta SQL

## Tickers padrão (B3)

| Ticker     | Empresa        |
|-----------|----------------|
| PETR4.SA  | Petrobras      |
| VALE3.SA  | Vale           |
| ITUB4.SA  | Itaú Unibanco  |
| BBDC4.SA  | Bradesco       |
| ABEV3.SA  | Ambev          |

Altere a lista `TICKERS` no notebook conforme necessário.

## Arquitetura

```
yfinance (API) → Produtor Python → /content/stock_stream/*.json
                                              ↓
                              Spark Structured Streaming
                                              ↓
                         MySQL local 127.0.0.1 (JDBC append)
```

## Observações

- Em produção, a fonte seria Kafka, Kinesis ou WebSocket da corretora; aqui simulamos com arquivos para funcionar no Colab sem infra extra.
- O driver JDBC MySQL é baixado automaticamente no notebook.
- Se reiniciar o runtime, execute todas as células novamente (MySQL precisa ser reinstalado).
- Pare o streaming com `query.stop()` ou interrompa a execução da célula.
