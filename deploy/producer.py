"""Produtor de votos da eleição Homer Simpson x Spock.

Publica votos no tópico Kafka `votos`. Cada voto tem candidato, estado (UF) e
event_time. Use com o docker-compose deste diretório (Kafka em localhost:9092).

Uso:
    python producer.py                # roda por DURACAO_SEG segundos
    KAFKA_BOOTSTRAP=host:9092 python producer.py
"""
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "votos")
DURACAO_SEG = int(os.getenv("DURACAO_SEG", "120"))
INTERVALO_SEG = float(os.getenv("INTERVALO_SEG", "2"))

CANDIDATOS = ["HOMER SIMPSON", "SPOCK"]
ESTADOS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
    "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
    "RS", "RO", "RR", "SC", "SP", "SE", "TO",
]
# Probabilidade de o voto ser no Homer em cada estado (eleição disputada).
PESO_HOMER = {uf: random.uniform(0.35, 0.65) for uf in ESTADOS}


def gerar_voto() -> dict:
    uf = random.choice(ESTADOS)
    candidato = "HOMER SIMPSON" if random.random() < PESO_HOMER[uf] else "SPOCK"
    return {
        "id_urna": random.randint(1, 500_000),
        "estado": uf,
        "candidato": candidato,
        "event_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"Produtor conectado em {KAFKA_BOOTSTRAP}, tópico '{KAFKA_TOPIC}'.")

    inicio = time.time()
    total = 0
    lote = 0
    try:
        while (time.time() - inicio) < DURACAO_SEG:
            lote += 1
            n = random.randint(20, 60)
            for _ in range(n):
                voto = gerar_voto()
                producer.send(KAFKA_TOPIC, key=voto["estado"], value=voto)
            producer.flush()
            total += n
            print(f"Lote {lote}: +{n} votos (acumulado: {total})")
            time.sleep(INTERVALO_SEG)
    finally:
        producer.flush()
        producer.close()
        print(f"Produtor encerrado. Total enviado: {total} votos.")


if __name__ == "__main__":
    main()
