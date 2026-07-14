"""Publica eventos de atualização do indicador no Pub/Sub.

Simula um sistema-fonte emitindo novas medições do Indicador Criança
Alfabetizada em tempo quase real. Garante que o tópico e a assinatura
existem antes de publicar.

Uso:
    python src/streaming/gerador_eventos.py            # 100 eventos
    python src/streaming/gerador_eventos.py --n 500
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import pydata_google_auth
from dotenv import load_dotenv
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

RAIZ_REPO = Path(__file__).resolve().parents[2]
load_dotenv(RAIZ_REPO / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
TOPICO = "eventos-indicador-alfabetizacao"
ASSINATURA = "eventos-indicador-sub"

PARQUET_MUNICIPIOS = RAIZ_REPO / "landing" / "indicador_municipio.parquet"


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # define o projeto de cota das chamadas de API
    return credenciais.with_quota_project(PROJECT_ID)


def garantir_infraestrutura(credenciais) -> None:
    """Cria tópico e assinatura caso não existam."""
    publisher = pubsub_v1.PublisherClient(credentials=credenciais)
    subscriber = pubsub_v1.SubscriberClient(credentials=credenciais)
    caminho_topico = publisher.topic_path(PROJECT_ID, TOPICO)
    caminho_assinatura = subscriber.subscription_path(PROJECT_ID, ASSINATURA)

    try:
        publisher.create_topic(request={"name": caminho_topico})
        print(f"[ok] tópico criado: {TOPICO}")
    except AlreadyExists:
        print(f"[ok] tópico já existe: {TOPICO}")

    try:
        subscriber.create_subscription(
            request={"name": caminho_assinatura, "topic": caminho_topico}
        )
        print(f"[ok] assinatura criada: {ASSINATURA}")
    except AlreadyExists:
        print(f"[ok] assinatura já existe: {ASSINATURA}")


def carregar_municipios() -> list[str]:
    """Usa códigos de municípios reais da landing, se disponível."""
    if PARQUET_MUNICIPIOS.exists():
        import pandas as pd

        df = pd.read_parquet(PARQUET_MUNICIPIOS, columns=["id_municipio"])
        return df["id_municipio"].dropna().unique().tolist()
    return ["3550308", "3304557", "5300108", "2927408", "2304400", "1302603"]


def publicar_eventos(credenciais, n_eventos: int) -> None:
    publisher = pubsub_v1.PublisherClient(credentials=credenciais)
    caminho_topico = publisher.topic_path(PROJECT_ID, TOPICO)
    municipios = carregar_municipios()

    print(f"[publicando] {n_eventos} eventos no tópico {TOPICO}...")
    for i in range(n_eventos):
        evento = {
            "tipo_evento": "atualizacao_indicador",
            "id_municipio": random.choice(municipios),
            "ano": 2024,
            "taxa_alfabetizacao": round(random.uniform(20.0, 95.0), 2),
            "timestamp_evento": datetime.now().isoformat(),
        }
        dados = json.dumps(evento).encode("utf-8")
        publisher.publish(caminho_topico, dados).result()
        if (i + 1) % 20 == 0:
            print(f"  {i + 1}/{n_eventos} eventos publicados")
        time.sleep(0.05)
    print("[ok] publicação concluída.")


if __name__ == "__main__":
    if not PROJECT_ID:
        sys.exit("Defina GCP_PROJECT_ID no arquivo .env.")
    parser = argparse.ArgumentParser(description="Gerador de eventos Pub/Sub")
    parser.add_argument("--n", type=int, default=100, help="número de eventos")
    args = parser.parse_args()

    credenciais = autenticar()
    garantir_infraestrutura(credenciais)
    publicar_eventos(credenciais, args.n)
