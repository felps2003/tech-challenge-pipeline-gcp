"""
Camada BRONZE — Tech Challenge Fase 2
======================================
Sobe os Parquets da pasta `landing/` para o Cloud Storage (data lake) e
os carrega no BigQuery (dataset `bronze`), preservando o dado bruto.

O que este script faz:
  1. Cria o bucket (se não existir) em região US — nível gratuito do GCS;
  2. Faz upload de cada Parquet para bronze/<tabela>/dt=<data>/ no bucket
     (a partição por data de ingestão preserva o histórico completo);
  3. Cria os datasets `bronze`, `silver` e `gold` no BigQuery;
  4. Carrega cada Parquet do bucket na tabela bronze.<tabela> via LOAD JOB
     (load jobs do BigQuery são GRATUITOS — decisão de FinOps).

Pré-requisitos:
  pip install -r requirements.txt   (google-cloud-storage e google-cloud-bigquery)
  Ter rodado antes: python src/ingestao/batch_basedosdados.py

Uso:
  export GCP_PROJECT_ID=seu-project-id     # ou edite abaixo
  export GCS_BUCKET=seu-bucket-unico       # nome globalmente único!
  python src/bronze/carregar_bronze.py
"""

import os
import sys
from datetime import date
from pathlib import Path

import pydata_google_auth
from google.cloud import bigquery, storage

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "conductive-coil-502322-f0")
BUCKET_NAME = os.getenv("GCS_BUCKET", "tech-challenge-alfabetizacao-felps")
REGIAO_BUCKET = "us-central1"   # região US = nível gratuito de 5 GB do GCS
LOCALIZACAO_BQ = "US"           # multi-região US, compatível com o bucket

RAIZ_REPO = Path(__file__).resolve().parents[2]
PASTA_LANDING = RAIZ_REPO / "landing"
DATA_INGESTAO = date.today().isoformat()

DATASETS = ["bronze", "silver", "gold", "governanca"]


def autenticar():
    """Autentica com sua conta Google (mesmo fluxo de navegador da ingestão)."""
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais


def garantir_bucket(cliente_gcs: storage.Client) -> storage.Bucket:
    """Cria o bucket se não existir (região US = nível gratuito)."""
    bucket = cliente_gcs.lookup_bucket(BUCKET_NAME)
    if bucket is None:
        bucket = cliente_gcs.create_bucket(BUCKET_NAME, location=REGIAO_BUCKET)
        print(f"[ok] bucket criado: gs://{BUCKET_NAME} ({REGIAO_BUCKET})")
    else:
        print(f"[ok] bucket já existe: gs://{BUCKET_NAME}")
    return bucket


def subir_parquets(bucket: storage.Bucket) -> list[tuple[str, str]]:
    """Sobe cada Parquet da landing para bronze/<tabela>/dt=<data>/."""
    enviados = []
    parquets = sorted(PASTA_LANDING.glob("*.parquet"))
    if not parquets:
        sys.exit(f"Nenhum Parquet em {PASTA_LANDING}. Rode a ingestão batch antes.")
    for arquivo in parquets:
        tabela = arquivo.stem
        destino = f"bronze/{tabela}/dt={DATA_INGESTAO}/{arquivo.name}"
        blob = bucket.blob(destino)
        blob.upload_from_filename(arquivo)
        print(f"[upload] {arquivo.name} -> gs://{BUCKET_NAME}/{destino}")
        enviados.append((tabela, destino))
    return enviados


def garantir_datasets(cliente_bq: bigquery.Client) -> None:
    """Cria os datasets das camadas (bronze/silver/gold + governanca)."""
    for nome in DATASETS:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{nome}")
        dataset.location = LOCALIZACAO_BQ
        cliente_bq.create_dataset(dataset, exists_ok=True)
        print(f"[ok] dataset pronto: {nome}")


def carregar_no_bigquery(cliente_bq: bigquery.Client, enviados: list) -> None:
    """Carrega cada Parquet do bucket na tabela bronze.<tabela>.

    Usa LOAD JOB (gratuito), com WRITE_TRUNCATE para recargas idempotentes.
    """
    for tabela, caminho_gcs in enviados:
        uri = f"gs://{BUCKET_NAME}/{caminho_gcs}"
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = cliente_bq.load_table_from_uri(uri, f"{PROJECT_ID}.bronze.{tabela}", job_config=config)
        job.result()  # espera terminar
        destino = cliente_bq.get_table(f"{PROJECT_ID}.bronze.{tabela}")
        print(f"[load] bronze.{tabela}: {destino.num_rows:,} linhas")


if __name__ == "__main__":
    credenciais = autenticar()
    cliente_gcs = storage.Client(project=PROJECT_ID, credentials=credenciais)
    cliente_bq = bigquery.Client(project=PROJECT_ID, credentials=credenciais)

    bucket = garantir_bucket(cliente_gcs)
    enviados = subir_parquets(bucket)
    garantir_datasets(cliente_bq)
    carregar_no_bigquery(cliente_bq, enviados)
    print("\nCamada Bronze concluída: dado bruto no GCS + tabelas bronze.* no BigQuery.")
