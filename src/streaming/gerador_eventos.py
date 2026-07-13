"""
Gerador de eventos — Ingestão STREAMING (Tech Challenge Fase 2)
================================================================
Simula um sistema-fonte publicando atualizações do Indicador Criança
Alfabetizada em tempo quase real, via Google Cloud Pub/Sub.

Cada evento é um JSON como:
  {
    "tipo_evento": "atualizacao_indicador",
    "id_municipio": "3550308",
    "ano": 2024,
    "taxa_alfabetizacao": 61.87,
    "timestamp_evento": "2026-07-13T14:22:05.123456"
  }

O script garante que o tópico E a assinatura existem antes de publicar
(sem assinatura criada, mensagens publicadas seriam perdidas).

Uso:
  export GCP_PROJECT_ID=seu-project-id
  python src/streaming/gerador_eventos.py            # publica 100 eventos
  python src/streaming/gerador_eventos.py --n 500    # publica 500
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
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "COLOQUE-SEU-PROJECT-ID-AQUI")
TOPICO = "eventos-indicador-alfabetizacao"
ASSINATURA = "eventos-indicador-sub"

RAIZ_REPO = Path(__file__).resolve().parents[2]
PARQUET_MUNICIPIOS = RAIZ_REPO / "landing" / "indicador_municipio.parquet"


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # Carimba o SEU projeto como consumidor da API (sem isso, o Pub/Sub
    # contabiliza a chamada no projeto do app OAuth e retorna 403).
    return credenciais.with_quota_project(PROJECT_ID)


def garantir_infraestrutura(credenciais) -> None:
    """Cria tópico e assinatura, se ainda não existirem."""
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
    """Usa municípios reais da landing, com fallback para capitais."""
    if PARQUET_MUNICIPIOS.exists():
        import pandas as pd

        df = pd.read_parquet(PARQUET_MUNICIPIOS, columns=["id_municipio"])
        return df["id_municipio"].dropna().unique().tolist()
    # fallback: algumas capitais (códigos IBGE)
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
        time.sleep(0.05)  # ~20 eventos/s, simulando fluxo contínuo
    print("[ok] publicação concluída.")


if __name__ == "__main__":
    if "COLOQUE" in PROJECT_ID:
        sys.exit("Configure GCP_PROJECT_ID (variável de ambiente ou edite o script).")
    parser = argparse.ArgumentParser(description="Gerador de eventos Pub/Sub")
    parser.add_argument("--n", type=int, default=100, help="número de eventos")
    args = parser.parse_args()

    credenciais = autenticar()
    garantir_infraestrutura(credenciais)
    publicar_eventos(credenciais, args.n)
