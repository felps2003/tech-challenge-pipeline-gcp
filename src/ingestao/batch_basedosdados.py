"""Ingestão batch do Indicador Criança Alfabetizada (Base dos Dados).

Extrai as tabelas do dataset br_inep_avaliacao_alfabetizacao e dos
diretórios territoriais do IBGE via BigQuery e salva em Parquet na
pasta landing/, sem transformações (camada bronze recebe o dado bruto).

Uso:
    python src/ingestao/batch_basedosdados.py            # extrai tudo
    python src/ingestao/batch_basedosdados.py --listar   # lista as tabelas do dataset
"""

import argparse
import os
import sys
import time
from pathlib import Path

import basedosdados as bd
from dotenv import load_dotenv

RAIZ_REPO = Path(__file__).resolve().parents[2]
load_dotenv(RAIZ_REPO / ".env")

BILLING_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")
PASTA_LANDING = RAIZ_REPO / "landing"
PASTA_AMOSTRAS = RAIZ_REPO / "data_samples"

DATASET_INDICADOR = "basedosdados.br_inep_avaliacao_alfabetizacao"
DATASET_DIRETORIOS = "basedosdados.br_bd_diretorios_brasil"

# Extração sem joins nem decodificação: a tradução dos códigos via tabela
# `dicionario` é responsabilidade da camada silver.
TABELAS = {
    "indicador_municipio": f"SELECT * FROM `{DATASET_INDICADOR}.municipio`",
    "indicador_uf":        f"SELECT * FROM `{DATASET_INDICADOR}.uf`",
    "meta_brasil":    f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_brasil`",
    "meta_uf":        f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_uf`",
    "meta_municipio": f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_municipio`",
    "alunos":         f"SELECT * FROM `{DATASET_INDICADOR}.alunos`",
    "dicionario":     f"SELECT * FROM `{DATASET_INDICADOR}.dicionario`",
    "diretorio_municipio": (
        "SELECT id_municipio, nome, sigla_uf, nome_regiao "
        f"FROM `{DATASET_DIRETORIOS}.municipio`"
    ),
    "diretorio_uf": f"SELECT sigla, nome, regiao FROM `{DATASET_DIRETORIOS}.uf`",
}


def listar_tabelas() -> None:
    """Lista as tabelas disponíveis no dataset do indicador."""
    query = f"""
        SELECT table_name
        FROM `{DATASET_INDICADOR}.INFORMATION_SCHEMA.TABLES`
        ORDER BY table_name
    """
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
    print(df.to_string(index=False))


def extrair() -> None:
    """Extrai cada tabela para Parquet, com amostra em data_samples/."""
    PASTA_LANDING.mkdir(parents=True, exist_ok=True)
    PASTA_AMOSTRAS.mkdir(parents=True, exist_ok=True)
    for nome, query in TABELAS.items():
        print(f"[extraindo] {nome} ...")
        # ate 3 tentativas por tabela, para absorver falhas de rede
        # em downloads longos
        for tentativa in range(1, 4):
            try:
                df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
                break
            except Exception as erro:
                if tentativa == 3:
                    raise
                print(f"[retry] {nome}: tentativa {tentativa} falhou "
                      f"({type(erro).__name__}) — nova tentativa em 15s...")
                time.sleep(15)
        destino = PASTA_LANDING / f"{nome}.parquet"
        df.to_parquet(destino, index=False)
        print(f"[ok] {nome}: {len(df):,} linhas -> {destino}")

        df.head(50).to_csv(PASTA_AMOSTRAS / f"{nome}_amostra.csv", index=False)
    print("\nIngestão batch concluída.")


if __name__ == "__main__":
    if not BILLING_PROJECT_ID:
        sys.exit("Defina GCP_PROJECT_ID no arquivo .env ou como variável de ambiente.")
    parser = argparse.ArgumentParser(description="Ingestão batch da Base dos Dados")
    parser.add_argument("--listar", action="store_true", help="lista as tabelas do dataset")
    args = parser.parse_args()

    if args.listar:
        listar_tabelas()
    else:
        extrair()
