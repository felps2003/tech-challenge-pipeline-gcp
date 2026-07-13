"""
Consumidor Pub/Sub — Ingestão STREAMING (Tech Challenge Fase 2)
================================================================
Consome os eventos de atualização do indicador publicados no Pub/Sub e
os aterrissa na camada Bronze em dois destinos:

  1. Data lake (GCS): arquivo JSONL em bronze/eventos_indicador/dt=<data>/
     — dado bruto preservado, mesmo padrão do batch;
  2. BigQuery: tabela bronze.eventos_indicador, via LOAD JOB (gratuito).

Decisão de FinOps: usamos load job em vez de streaming insert do BigQuery
(que é cobrado). Em produção com alta frequência, o caminho seria
Storage Write API ou Dataflow — documentado como trade-off no README.

Uso:
  export GCP_PROJECT_ID=seu-project-id
  export GCS_BUCKET=seu-bucket
  python src/streaming/consumidor_pubsub.py
"""

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pydata_google_auth
from google.api_core.exceptions import DeadlineExceeded
from google.cloud import bigquery, pubsub_v1, storage

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "COLOQUE-SEU-PROJECT-ID-AQUI")
BUCKET_NAME = os.getenv("GCS_BUCKET", "COLOQUE-SEU-BUCKET-AQUI")
ASSINATURA = "eventos-indicador-sub"
TABELA_DESTINO = "bronze.eventos_indicador"
LOTE_MAXIMO = 100          # mensagens por pull
PULLS_VAZIOS_PARA_PARAR = 3

RAIZ_REPO = Path(__file__).resolve().parents[2]
PASTA_EVENTOS = RAIZ_REPO / "landing" / "eventos"


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # Carimba o SEU projeto como consumidor da API (sem isso, o Pub/Sub
    # contabiliza a chamada no projeto do app OAuth e retorna 403).
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
                retry=None,  # sem retry interno: timeout = fila vazia
            )
        except DeadlineExceeded:
            # Fila vazia: o Pub/Sub deixou o pull esperar até estourar o tempo.
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
        subscriber.acknowledge(request={"subscription": caminho, "ack_ids": ack_ids})
        print(f"  {len(eventos)} eventos consumidos até agora...")
    print(f"[ok] fila vazia. Total: {len(eventos)} eventos.")
    return eventos


def aterrissar(credenciais, eventos: list[dict]) -> None:
    """Grava os eventos no GCS (bruto) e no BigQuery (load job gratuito)."""
    if not eventos:
        print("Nenhum evento para aterrissar. Rode o gerador antes.")
        return

    # 1) arquivo JSONL local (auditoria) e no bucket (data lake)
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

    # 2) BigQuery via load job (gratuito), com append para acumular histórico
    cliente_bq = bigquery.Client(project=PROJECT_ID, credentials=credenciais)
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    uri = f"gs://{BUCKET_NAME}/{destino_gcs}"
    job = cliente_bq.load_table_from_uri(uri, f"{PROJECT_ID}.{TABELA_DESTINO}", job_config=config)
    job.result()
    tabela = cliente_bq.get_table(f"{PROJECT_ID}.{TABELA_DESTINO}")
    print(f"[load] {TABELA_DESTINO}: {tabela.num_rows:,} linhas acumuladas")


if __name__ == "__main__":
    if "COLOQUE" in PROJECT_ID or "COLOQUE" in BUCKET_NAME:
        sys.exit("Configure GCP_PROJECT_ID e GCS_BUCKET antes de rodar.")
    credenciais = autenticar()
    eventos = consumir(credenciais)
    aterrissar(credenciais, eventos)
    print("\nIngestão streaming concluída: eventos brutos no GCS e no BigQuery.")
