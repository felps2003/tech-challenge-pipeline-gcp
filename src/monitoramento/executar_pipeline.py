"""
Orquestrador + Monitoramento — Tech Challenge Fase 2
=====================================================
Executa o pipeline completo, na ordem correta, monitorando cada etapa:

  1. Ingestão batch (Base dos Dados -> landing/)
  2. Camada Bronze (landing/ -> GCS -> BigQuery)
  3. Camada Silver (limpeza + integração)
  4. Validações de qualidade
  5. Camada Gold (tabelas analíticas)

Observabilidade implementada (requisitos do edital):
  - Falhas de ingestão .... status por etapa + interrupção em erro
  - Latência ............. duração em segundos de cada etapa
  - Volume ............... resumo de linhas por tabela ao final
  - Alertas .............. exit code != 0 e mensagem destacada em falha
    (em produção: job no Cloud Scheduler + alerta do Cloud Monitoring)

Tudo é gravado em governanca.log_execucoes via LOAD JOB (gratuito).

Uso:
  export GCP_PROJECT_ID=...   e   export GCS_BUCKET=...
  python src/monitoramento/executar_pipeline.py             # pipeline completo
  python src/monitoramento/executar_pipeline.py --sem-batch # pula a re-extração
"""

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pydata_google_auth
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "COLOQUE-SEU-PROJECT-ID-AQUI")
RAIZ_REPO = Path(__file__).resolve().parents[2]

ETAPAS = [
    ("ingestao_batch",  [sys.executable, "src/ingestao/batch_basedosdados.py"]),
    ("camada_bronze",   [sys.executable, "src/bronze/carregar_bronze.py"]),
    ("camada_silver",   [sys.executable, "src/executar_sql.py", "src/silver/transformacoes.sql"]),
    ("qualidade_dados", [sys.executable, "src/executar_sql.py", "src/qualidade/validacoes.sql"]),
    ("camada_gold",     [sys.executable, "src/executar_sql.py", "src/gold/tabelas_analiticas.sql"]),
]

TABELAS_VOLUME = [
    "bronze.indicador_municipio", "bronze.alunos", "bronze.eventos_indicador",
    "silver.indicador_integrado", "gold.indicador_por_municipio",
    "gold.meta_vs_resultado", "gold.evolucao_temporal",
]


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais.with_quota_project(PROJECT_ID)


def registrar_execucoes(cliente_bq: bigquery.Client, registros: list[dict]) -> None:
    """Grava o log da execução no BigQuery (load job = gratuito)."""
    config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = cliente_bq.load_table_from_json(
        registros, f"{PROJECT_ID}.governanca.log_execucoes", job_config=config
    )
    job.result()


def resumo_volume(cliente_bq: bigquery.Client) -> None:
    """Volume de dados processados — visão rápida por camada."""
    print("\n--- VOLUME POR TABELA ---")
    for tabela in TABELAS_VOLUME:
        try:
            t = cliente_bq.get_table(f"{PROJECT_ID}.{tabela}")
            print(f"  {tabela:<38} {t.num_rows:>12,} linhas")
        except Exception:
            print(f"  {tabela:<38} {'(nao existe)':>12}")


def executar(pular_batch: bool) -> None:
    id_execucao = datetime.now().strftime("%Y%m%d_%H%M%S")
    registros, houve_falha = [], False

    etapas = [e for e in ETAPAS if not (pular_batch and e[0] == "ingestao_batch")]

    print(f"=== PIPELINE {id_execucao} — {len(etapas)} etapas ===\n")
    for nome, comando in etapas:
        print(f">>> [{nome}] iniciando...")
        inicio = time.time()
        processo = subprocess.run(comando, cwd=RAIZ_REPO)
        duracao = round(time.time() - inicio, 1)
        status = "OK" if processo.returncode == 0 else "FALHA"

        registros.append({
            "id_execucao": id_execucao,
            "etapa": nome,
            "status": status,
            "duracao_segundos": duracao,
            "inicio": datetime.now().isoformat(),
        })
        print(f">>> [{nome}] {status} em {duracao}s\n")

        if status == "FALHA":
            houve_falha = True
            print(f"!!! ALERTA: etapa '{nome}' falhou — pipeline interrompido. !!!")
            break  # não continua com dados possivelmente inconsistentes

    credenciais = autenticar()
    cliente_bq = bigquery.Client(project=PROJECT_ID, credentials=credenciais)
    registrar_execucoes(cliente_bq, registros)
    print(f"[ok] log gravado em governanca.log_execucoes (id {id_execucao})")

    if not houve_falha:
        resumo_volume(cliente_bq)

    total = sum(r["duracao_segundos"] for r in registros)
    print(f"\n=== FIM — {len(registros)} etapas em {total:.0f}s — "
          f"{'COM FALHA' if houve_falha else 'sucesso'} ===")
    sys.exit(1 if houve_falha else 0)


if __name__ == "__main__":
    if "COLOQUE" in PROJECT_ID:
        sys.exit("Configure GCP_PROJECT_ID antes de rodar.")
    parser = argparse.ArgumentParser(description="Orquestrador do pipeline")
    parser.add_argument("--sem-batch", action="store_true",
                        help="pula a re-extração da Base dos Dados")
    args = parser.parse_args()
    executar(args.sem_batch)
