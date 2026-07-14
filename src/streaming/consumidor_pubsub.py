"""Consome os eventos do Pub/Sub e os aterrissa na camada bronze.

Destinos do dado bruto:
  1. Cloud Storage: JSONL em bronze/eventos_indicador/dt=<data>/;
  2. BigQuery: tabela bronze.eventos_indicador, via load job.

Uso:
    python src/streaming/consumidor_pubsub.py
"""

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pydata_google_auth
from dotenv import load_dotenv
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery, pubsub_v1, storage

RAIZ_REPO = Path(__file__).resolve().parents[2]
load_dotenv(RAIZ_REPO / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BUCKET_NAME = os.getenv("GCS_BUCKET", "")
ASSINATURA = "eventos-indicador-sub"
TABELA_DESTINO = "bronze.eventos_indicador"
LOTE_MAXIMO = 100
PULLS_VAZIOS_PARA_PARAR = 3

PASTA_EVENTOS = RAIZ_REPO / "landing" / "eventos"


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais.with_quota_project(PROJECT_ID)


def consumir(credenciais) -> list[dict]:
    """Puxa mensagens da assinatura até a fila esvaziar."""
    subscriber = pubsub_v1.SubscriberClient(credentials=credenciais)
    caminho = subscriber.subscription_path(PROJECT_ID, ASSINATURA)
    eventos, pulls_vazios = [], 0

    print(f"[consumindo] assinatura {ASSINATURA}...")
    while pulls_vazios < PULLS_VAZIOS_PARA_PARAR:
        try:
            resposta = subscriber.pull(
                request={"subscription": caminho, "max_messages": LOTE_MAXIMO},
                timeout=10,
                retry=None,
            )
        except DeadlineExceeded:
            # fila vazia: o pull síncrono expira sem retornar mensagens
            pulls_vazios += 1
            continue
        if not resposta.received_messages:
            pulls_vazios += 1
            continue
        pulls_vazios = 0
        ack_ids = []
        for msg in resposta.received_messages:
            evento = json.loads(msg.message.data.decode("utf-8"))
            evento["_consumido_ts"] = datetime.now().isoformat()
            eventos.append(evento)
            ack_ids.append(msg.ack_id)
        # ack somente após o processamento do lote
        subscriber.acknowledge(request={"subscription": caminho, "ack_ids": ack_ids})
        print(f"  {len(eventos)} eventos consumidos até agora...")
    print(f"[ok] fila vazia. Total: {len(eventos)} eventos.")
    return eventos


def aterrissar(credenciais, eventos: list[dict]) -> None:
    """Grava os eventos brutos no GCS e no BigQuery."""
    if not eventos:
        print("Nenhum evento para aterrissar.")
        return

    PASTA_EVENTOS.mkdir(parents=True, exist_ok=True)
    carimbo = datetime.now().strftime("%Y%m%d_%H%M%S")
    arquivo = PASTA_EVENTOS / f"eventos_{carimbo}.jsonl"
    with open(arquivo, "w", encoding="utf-8") as f:
        for e in eventos:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")

    cliente_gcs = storage.Client(project=PROJECT_ID, credentials=credenciais)
    destino_gcs = f"bronze/eventos_indicador/dt={date.today().isoformat()}/{arquivo.name}"
    cliente_gcs.bucket(BUCKET_NAME).blob(destino_gcs).upload_from_filename(arquivo)
    print(f"[upload] gs://{BUCKET_NAME}/{destino_gcs}")

    cliente_bq = bigquery.Client(project=PROJECT_ID, credentials=credenciais)
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    uri = f"gs://{BUCKET_NAME}/{destino_gcs}"
    job = cliente_bq.load_table_from_uri(
        uri, f"{PROJECT_ID}.{TABELA_DESTINO}", job_config=config
    )
    job.result()
    tabela = cliente_bq.get_table(f"{PROJECT_ID}.{TABELA_DESTINO}")
    print(f"[load] {TABELA_DESTINO}: {tabela.num_rows:,} linhas acumuladas")


if __name__ == "__main__":
    if not PROJECT_ID or not BUCKET_NAME:
        sys.exit("Defina GCP_PROJECT_ID e GCS_BUCKET no arquivo .env.")
    credenciais = autenticar()
    eventos = consumir(credenciais)
    aterrissar(credenciais, eventos)
    print("\nIngestão streaming concluída.")
