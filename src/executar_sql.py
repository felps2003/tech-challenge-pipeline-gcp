"""Executa um arquivo .sql (comandos separados por `;`) no BigQuery.

Substitui o token ${PROJECT_ID} pelo projeto configurado e imprime os
bytes processados por comando. Comandos SELECT têm o resultado exibido
no terminal (até 30 linhas).

Uso:
    python src/executar_sql.py src/silver/transformacoes.sql
"""

import os
import sys
from pathlib import Path

import pydata_google_auth
from dotenv import load_dotenv
from google.cloud import bigquery

RAIZ_REPO = Path(__file__).resolve().parents[1]
load_dotenv(RAIZ_REPO / ".env")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "")


def autenticar():
    credenciais = pydata_google_auth.get_user_credentials(
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return credenciais.with_quota_project(PROJECT_ID)


def executar_arquivo(caminho_sql: str) -> None:
    sql = Path(caminho_sql).read_text(encoding="utf-8")
    sql = sql.replace("${PROJECT_ID}", PROJECT_ID)

    # separa por `;` e descarta linhas de comentário no início de cada bloco
    comandos = []
    for bloco in sql.split(";"):
        linhas = bloco.splitlines()
        while linhas and (not linhas[0].strip() or linhas[0].strip().startswith("--")):
            linhas.pop(0)
        comando = "\n".join(linhas).strip()
        if comando:
            comandos.append(comando)

    cliente = bigquery.Client(project=PROJECT_ID, credentials=autenticar())

    total_bytes = 0
    for i, comando in enumerate(comandos, start=1):
        rotulo = next(
            (l.strip() for l in comando.splitlines()
             if l.strip() and not l.strip().startswith("--")),
            f"comando {i}",
        )
        print(f"[{i}/{len(comandos)}] {rotulo[:80]}...")
        job = cliente.query(comando)
        resultado = job.result()
        processado = job.total_bytes_processed or 0
        total_bytes += processado
        print(f"    ok — {processado / 1e6:.1f} MB processados")

        if comando.lstrip().upper().startswith("SELECT"):
            linhas = list(resultado)
            print(f"    --- resultado ({len(linhas)} linhas) ---")
            for linha in linhas[:30]:
                print("    " + " | ".join(str(v) for v in linha.values()))

    print(f"\nConcluído: {len(comandos)} comandos, {total_bytes / 1e6:.1f} MB processados.")


if __name__ == "__main__":
    if not PROJECT_ID:
        sys.exit("Defina GCP_PROJECT_ID no arquivo .env.")
    if len(sys.argv) < 2:
        sys.exit("Uso: python src/executar_sql.py <arquivo.sql>")
    executar_arquivo(sys.argv[1])
