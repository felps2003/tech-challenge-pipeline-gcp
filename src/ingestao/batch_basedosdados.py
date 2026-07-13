"""
Ingestão BATCH — Tech Challenge Fase 2
=======================================
Extrai as entidades obrigatórias do pdf a partir do datalake público
da Base dos Dados (BigQuery) e as salva em Parquet na pasta `landing/`,
prontas para subir à camada Bronze (bucket do Cloud Storage).

Entidades cobertas:
  - Indicador Criança Alfabetizada por Município / UF / Brasil
    (dataset: basedosdados.br_inep_avaliacao_alfabetizacao)
  - Diretório de Municípios e UFs (dados territoriais)
    (dataset: basedosdados.br_bd_diretorios_brasil)
"""

import argparse
import os
import sys
from pathlib import Path

import basedosdados as bd

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
BILLING_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "conductive-coil-502322-f0")

RAIZ_REPO = Path(__file__).resolve().parents[2]
PASTA_LANDING = RAIZ_REPO / "landing"
PASTA_AMOSTRAS = RAIZ_REPO / "data_samples"

DATASET_INDICADOR = "basedosdados.br_inep_avaliacao_alfabetizacao"
DATASET_DIRETORIOS = "basedosdados.br_bd_diretorios_brasil"

# Tabela -> query de extração.
# Filosofia Bronze: dados BRUTOS (SELECT *), sem joins nem decodificação.
# A tradução de códigos via tabela `dicionario` acontece na camada Silver.
TABELAS = {
    # Indicador Criança Alfabetizada (entidades: Município e UF)
    "indicador_municipio": f"SELECT * FROM `{DATASET_INDICADOR}.municipio`",
    "indicador_uf":        f"SELECT * FROM `{DATASET_INDICADOR}.uf`",

    # Metas de alfabetização (entidades: Brasil, UF e Município)
    "meta_brasil":    f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_brasil`",
    "meta_uf":        f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_uf`",
    "meta_municipio": f"SELECT * FROM `{DATASET_INDICADOR}.meta_alfabetizacao_municipio`",

    # Microdados de alunos (entidade: Dados de alunos) — tabela grande,
    # a extração pode levar alguns minutos.
    "alunos": f"SELECT * FROM `{DATASET_INDICADOR}.alunos`",

    # Dicionário de códigos (série, rede, presença...) — usado na Silver
    "dicionario": f"SELECT * FROM `{DATASET_INDICADOR}.dicionario`",

    # Dados territoriais (diretórios do IBGE via Base dos Dados)
    "diretorio_municipio": (
        "SELECT id_municipio, nome, sigla_uf, nome_regiao "
        f"FROM `{DATASET_DIRETORIOS}.municipio`"
    ),
    "diretorio_uf": f"SELECT sigla, nome, regiao FROM `{DATASET_DIRETORIOS}.uf`",
}

def extrair() -> None:
    """Extrai cada tabela e salva em Parquet (formato colunar = FinOps)."""
    PASTA_LANDING.mkdir(parents=True, exist_ok=True)
    PASTA_AMOSTRAS.mkdir(parents=True, exist_ok=True)
    for nome, query in TABELAS.items():
        print(f"[extraindo] {nome} ...")
        df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
        destino = PASTA_LANDING / f"{nome}.parquet"
        df.to_parquet(destino, index=False)
        print(f"[ok] {nome}: {len(df):,} linhas -> {destino}")

        # Amostra pequena para o repositório (avaliador consegue ver o dado)
        amostra = PASTA_AMOSTRAS / f"{nome}_amostra.csv"
        df.head(50).to_csv(amostra, index=False)
    print("\nIngestão batch concluída. Próximo passo: subir landing/ para o bucket (Bronze).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão batch da Base dos Dados")
    parser.add_argument("--listar", action="store_true", help="só lista as tabelas do dataset")
    args = parser.parse_args()
    extrair()