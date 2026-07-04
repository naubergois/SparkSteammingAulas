# Spark Streaming — Cotações de Bolsa → MySQL

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/naubergois/SparkSteammingAulas/blob/main/exercicio_spark_streaming_bolsa_mysql.ipynb)

Exercício prático para Google Colab: simula um fluxo contínuo de cotações de ações (B3) com **Apache Spark Structured Streaming** e grava os dados em **MySQL instalado localmente no Colab**.

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
2. Executar o notebook — **não precisa de banco externo nem Azure**

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
2. **Runtime → Run all** — sem configurar secrets
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
