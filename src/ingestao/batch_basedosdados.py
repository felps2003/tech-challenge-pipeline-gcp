"""
Ingestão BATCH — Tech Challenge Fase 2
=======================================
Extrai as entidades obrigatórias do edital a partir do datalake público
da Base dos Dados (BigQuery) e as salva em Parquet na pasta `landing/`,
prontas para subir à camada Bronze (bucket do Cloud Storage).

Entidades cobertas:
  - Indicador Criança Alfabetizada por Município / UF / Brasil
    (dataset: basedosdados.br_inep_avaliacao_alfabetizacao)
  - Diretório de Municípios e UFs (dados territoriais)
    (dataset: basedosdados.br_bd_diretorios_brasil)

Pré-requisitos:
  pip install basedosdados pandas pyarrow
  Projeto GCP criado (o ID vai em BILLING_PROJECT_ID abaixo ou na
  variável de ambiente GCP_PROJECT_ID). Na primeira execução, o pacote
  abre o navegador para você autorizar com sua conta Google.

Uso:
  python src/ingestao/batch_basedosdados.py            # extrai tudo
  python src/ingestao/batch_basedosdados.py --listar   # só lista as tabelas do dataset
"""

import argparse
import os
import sys
import time
from pathlib import Path

import basedosdados as bd

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
BILLING_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "COLOQUE-SEU-PROJECT-ID-AQUI")

# Caminhos ancorados na raiz do repositório (2 níveis acima deste arquivo),
# para funcionar de qualquer diretório em que o script seja executado.
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


def listar_tabelas() -> None:
    """Descobre as tabelas disponíveis no dataset do indicador.

    Rode este comando primeiro: ele confirma os nomes reais das tabelas
    (municipio, uf, brasil, metas...) antes de extrair.
    """
    query = f"""
        SELECT table_name
        FROM `{DATASET_INDICADOR}.INFORMATION_SCHEMA.TABLES`
        ORDER BY table_name
    """
    df = bd.read_sql(query, billing_project_id=BILLING_PROJECT_ID)
    print("Tabelas disponíveis no dataset do indicador:")
    print(df.to_string(index=False))


def extrair() -> None:
    """Extrai cada tabela e salva em Parquet (formato colunar = FinOps)."""
    PASTA_LANDING.mkdir(parents=True, exist_ok=True)
    PASTA_AMOSTRAS.mkdir(parents=True, exist_ok=True)
    for nome, query in TABELAS.items():
        print(f"[extraindo] {nome} ...")
        # Resiliência: até 3 tentativas por tabela (falhas de rede acontecem
        # em downloads longos — detectado pelo monitoramento do pipeline)
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

        # Amostra pequena para o repositório (avaliador consegue ver o dado)
        amostra = PASTA_AMOSTRAS / f"{nome}_amostra.csv"
        df.head(50).to_csv(amostra, index=False)
    print("\nIngestão batch concluída. Próximo passo: subir landing/ para o bucket (Bronze).")


if __name__ == "__main__":
    if "COLOQUE-SEU" in BILLING_PROJECT_ID:
        sys.exit(
            "Configure seu projeto GCP: edite BILLING_PROJECT_ID no script "
            "ou exporte a variável GCP_PROJECT_ID."
        )
    parser = argparse.ArgumentParser(description="Ingestão batch da Base dos Dados")
    parser.add_argument("--listar", action="store_true", help="só lista as tabelas do dataset")
    args = parser.parse_args()

    if args.listar:
        listar_tabelas()
    else:
        extrair()
