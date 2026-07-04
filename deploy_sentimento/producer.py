"""Coleta posts do X (@diariodonordeste) e grava no stream para o Spark.

Com X_BEARER_TOKEN definido, busca os posts reais via API v2 do X. Sem token,
usa posts de exemplo. Cada post vira um arquivo JSON em STREAM_DIR.

Uso:
    export X_BEARER_TOKEN="SEU_TOKEN"   # opcional
    python producer.py
"""
import json
import os
import time
from datetime import datetime
from pathlib import Path

import requests

CONTA_X = os.getenv("CONTA_X", "diariodonordeste")
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN", "")
STREAM_DIR = Path(os.getenv("STREAM_DIR", "./posts_stream"))
INTERVALO_SEG = float(os.getenv("INTERVALO_SEG", "1.5"))
MAX_POSTS = int(os.getenv("MAX_POSTS", "20"))

POSTS_EXEMPLO = [
    "Fortaleza inaugura novo hospital com 200 leitos e amplia atendimento no Ceará",
    "Chuvas fortes causam alagamentos e deixam famílias desabrigadas no interior",
    "Ceará bate recorde de energia solar e lidera geração renovável no Nordeste",
    "Acidente grave na BR-116 deixa feridos e interdita a rodovia por horas",
    "Turismo cresce no litoral cearense e aquece a economia local nas férias",
    "Assalto a banco assusta moradores no Centro de Fortaleza nesta manhã",
    "Estudantes do Ceará conquistam medalhas em olimpíada nacional de matemática",
    "Falta de água atinge bairros e população reclama do abastecimento irregular",
    "Novo parque é entregue à população e vira opção de lazer na capital",
    "Preço dos alimentos sobe e pesa no orçamento das famílias cearenses",
    "Seleção de vôlei do Ceará vence e garante vaga na final do campeonato",
    "Incêndio destrói galpão e mobiliza bombeiros na Região Metropolitana",
]


def _exemplos():
    return [
        {"id": f"exemplo_{i}", "texto": t, "criado_em": datetime.utcnow().isoformat()}
        for i, t in enumerate(POSTS_EXEMPLO)
    ]


def coletar_posts_x():
    if not X_BEARER_TOKEN:
        return _exemplos()
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    try:
        u = requests.get(
            f"https://api.twitter.com/2/users/by/username/{CONTA_X}",
            headers=headers, timeout=20,
        )
        u.raise_for_status()
        user_id = u.json()["data"]["id"]
        r = requests.get(
            f"https://api.twitter.com/2/users/{user_id}/tweets",
            headers=headers,
            params={"max_results": MAX_POSTS, "tweet.fields": "created_at",
                    "exclude": "retweets,replies"},
            timeout=20,
        )
        r.raise_for_status()
        return [
            {"id": t["id"], "texto": t["text"],
             "criado_em": t.get("created_at", datetime.utcnow().isoformat())}
            for t in r.json().get("data", [])
        ]
    except Exception as e:
        print(f"Falha ao coletar do X ({e}). Usando posts de exemplo.")
        return _exemplos()


def main() -> None:
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    posts = coletar_posts_x()
    print(f"Coletados {len(posts)} posts (fonte: {'X API' if X_BEARER_TOKEN else 'exemplos'}).")
    for i, post in enumerate(posts):
        arq = STREAM_DIR / f"post_{i:03d}.json"
        arq.write_text(json.dumps(post, ensure_ascii=False), encoding="utf-8")
        print(f"enviado: {post['texto'][:70]}...")
        time.sleep(INTERVALO_SEG)
    print("Produtor encerrado.")


if __name__ == "__main__":
    main()
