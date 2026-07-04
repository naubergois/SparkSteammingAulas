# Spark Streaming — Cotações de Bolsa → MySQL

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_bolsa_mysql.ipynb)

Exercício prático para Google Colab: simula um fluxo contínuo de cotações de ações (B3) com **Apache Spark Structured Streaming** e grava os dados em **MySQL**.

## Abrir no Colab Research

Clique no badge acima ou acesse diretamente:

**https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_bolsa_mysql.ipynb**

## O que você vai aprender

- Buscar cotações em tempo quasi-real com `yfinance`
- Simular um stream de dados (pasta monitorada pelo Spark)
- Processar micro-batches com Structured Streaming
- Persistir no MySQL via JDBC (`foreachBatch`)

## Pré-requisitos

1. Conta no [Google Colab](https://colab.research.google.com/)
2. Instância MySQL acessível pela internet (ex.: [PlanetScale](https://planetscale.com/), [Railway](https://railway.app/), [Aiven](https://aiven.io/) ou MySQL local + [ngrok](https://ngrok.com/))
3. Abrir o notebook `exercicio_spark_streaming_bolsa_mysql.ipynb` no Colab

## Configuração do MySQL

Execute no seu banco antes de rodar o notebook:

```sql
CREATE DATABASE IF NOT EXISTS bolsa CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE bolsa;

CREATE TABLE IF NOT EXISTS cotacoes (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker        VARCHAR(20)  NOT NULL,
    preco         DECIMAL(18,4) NOT NULL,
    abertura      DECIMAL(18,4),
    maxima        DECIMAL(18,4),
    minima        DECIMAL(18,4),
    volume        BIGINT,
    moeda         VARCHAR(10),
    bolsa         VARCHAR(50),
    coletado_em   DATETIME     NOT NULL,
    processado_em TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_ticker_data (ticker, coletado_em)
);
```

O notebook também cria a tabela automaticamente via Python.

## Como usar no Colab

1. Clique no badge **Open in Colab** (ou no link acima)
2. Configure os secrets em **🔑 Secrets** (ícone de chave à esquerda):
   - `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`
3. Execute todas as células em ordem (**Runtime → Run all**)
4. O **produtor** roda em background por ~2 minutos gerando arquivos JSON
5. O **Spark Streaming** consome os arquivos e grava no MySQL
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
                                    MySQL (JDBC append)
```

## Observações

- Em produção, a fonte seria Kafka, Kinesis ou WebSocket da corretora; aqui simulamos com arquivos para funcionar no Colab sem infra extra.
- O driver JDBC MySQL é baixado automaticamente no notebook.
- Pare o streaming com `query.stop()` ou interrompa a execução da célula.
