"""Camada bronze: landing/ -> Cloud Storage -> BigQuery.

Sobe os Parquets da landing para o data lake (particionados por data de
ingestão) e os carrega no dataset bronze do BigQuery via load jobs.
Também cria o bucket e os datasets das camadas na primeira execução.

Uso:
    python src/bronze/carregar_bronze.py
"""

import os
import sys
from datetime import date
from pathlib import Path

import pydata_google_auth
from dotenv import load_dotenv
from google.cloud import bigquery, storage

RAIZ_REPO = Path(__file__).resolve().parents[2]
load_dotenv(RAIZ_REPO / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
BUCKET_NAME = os.getenv("GCS_BUCKET", "")
REGIAO_BUCKET = "us-central1"
LOCALIZACAO_BQ = "US"

PASTA_LANDING = RAIZ_REPO / "landing"
DATA_INGESTAO = date.today().isoformat()

DATASETS = ["bronze", "silver", "gold", "governanca"]


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais.with_quota_project(PROJECT_ID)


def garantir_bucket(cliente_gcs: storage.Client) -> storage.Bucket:
    bucket = cliente_gcs.lookup_bucket(BUCKET_NAME)
    if bucket is None:
        bucket = cliente_gcs.create_bucket(BUCKET_NAME, location=REGIAO_BUCKET)
        print(f"[ok] bucket criado: gs://{BUCKET_NAME} ({REGIAO_BUCKET})")
    else:
        print(f"[ok] bucket já existe: gs://{BUCKET_NAME}")
    return bucket


def subir_parquets(bucket: storage.Bucket) -> list[tuple[str, str]]:
    """Envia cada Parquet para bronze/<tabela>/dt=<data de ingestão>/."""
    enviados = []
    parquets = sorted(PASTA_LANDING.glob("*.parquet"))
    if not parquets:
        sys.exit(f"Nenhum Parquet em {PASTA_LANDING}. Rode a ingestão batch antes.")
    for arquivo in parquets:
        tabela = arquivo.stem
        destino = f"bronze/{tabela}/dt={DATA_INGESTAO}/{arquivo.name}"
        bucket.blob(destino).upload_from_filename(arquivo)
        print(f"[upload] {arquivo.name} -> gs://{BUCKET_NAME}/{destino}")
        enviados.append((tabela, destino))
    return enviados


def garantir_datasets(cliente_bq: bigquery.Client) -> None:
    for nome in DATASETS:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{nome}")
        dataset.location = LOCALIZACAO_BQ
        cliente_bq.create_dataset(dataset, exists_ok=True)
        print(f"[ok] dataset pronto: {nome}")


def carregar_no_bigquery(cliente_bq: bigquery.Client, enviados: list) -> None:
    """Carrega cada Parquet na tabela bronze correspondente (load job)."""
    for tabela, caminho_gcs in enviados:
        uri = f"gs://{BUCKET_NAME}/{caminho_gcs}"
        config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        job = cliente_bq.load_table_from_uri(
            uri, f"{PROJECT_ID}.bronze.{tabela}", job_config=config
        )
        job.result()
        destino = cliente_bq.get_table(f"{PROJECT_ID}.bronze.{tabela}")
        print(f"[load] bronze.{tabela}: {destino.num_rows:,} linhas")


if __name__ == "__main__":
    if not PROJECT_ID or not BUCKET_NAME:
        sys.exit("Defina GCP_PROJECT_ID e GCS_BUCKET no arquivo .env.")
    credenciais = autenticar()
    cliente_gcs = storage.Client(project=PROJECT_ID, credentials=credenciais)
    cliente_bq = bigquery.Client(project=PROJECT_ID, credentials=credenciais)

    bucket = garantir_bucket(cliente_gcs)
    enviados = subir_parquets(bucket)
    garantir_datasets(cliente_bq)
    carregar_no_bigquery(cliente_bq, enviados)
    print("\nCamada bronze concluída.")
