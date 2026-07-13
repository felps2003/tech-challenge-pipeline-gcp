"""
Executor de SQL no BigQuery — Tech Challenge Fase 2
====================================================
Roda um arquivo .sql (com vários comandos separados por `;`) no BigQuery,
substituindo o token ${PROJECT_ID} pelo projeto configurado.

Também imprime os bytes processados por comando — evidência de FinOps
(as consultas saem da cota gratuita de 1 TiB/mês do BigQuery).

Uso:
  export GCP_PROJECT_ID=seu-project-id
  python src/executar_sql.py src/silver/transformacoes.sql
"""

import os
import sys
from pathlib import Path

import pydata_google_auth
from google.cloud import bigquery

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "COLOQUE-SEU-PROJECT-ID-AQUI")


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais.with_quota_project(PROJECT_ID)


def executar_arquivo(caminho_sql: str) -> None:
    sql = Path(caminho_sql).read_text(encoding="utf-8")
    sql = sql.replace("${PROJECT_ID}", PROJECT_ID)

    # separa por `;` e remove linhas que são só comentário no início de cada bloco
    comandos = []
    for bloco in sql.split(";"):
        linhas = [l for l in bloco.splitlines()]
        while linhas and (not linhas[0].strip() or linhas[0].strip().startswith("--")):
            linhas.pop(0)
        comando = "\n".join(linhas).strip()
        if comando:
            comandos.append(comando)
    cliente = bigquery.Client(project=PROJECT_ID, credentials=autenticar())

    total_bytes = 0
    for i, comando in enumerate(comandos, start=1):
        # primeira linha não-comentário, para exibir no log
        rotulo = next(
            (l.strip() for l in comando.splitlines() if l.strip() and not l.strip().startswith("--")),
            f"comando {i}",
        )
        print(f"[{i}/{len(comandos)}] {rotulo[:80]}...")
        job = cliente.query(comando)
        job.result()
        processado = job.total_bytes_processed or 0
        total_bytes += processado
        print(f"    ok — {processado / 1e6:.1f} MB processados")

    print(f"\nConcluído: {len(comandos)} comandos, {total_bytes / 1e6:.1f} MB no total "
          f"(cota gratuita: 1 TiB/mês).")


if __name__ == "__main__":
    if "COLOQUE" in PROJECT_ID:
        sys.exit("Configure GCP_PROJECT_ID antes de rodar.")
    if len(sys.argv) < 2:
        sys.exit("Uso: python src/executar_sql.py <arquivo.sql>")
    executar_arquivo(sys.argv[1])
