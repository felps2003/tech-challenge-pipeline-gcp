-- ============================================================================
-- CAMADA SILVER — Tech Challenge Fase 2
-- Limpeza, padronização, decodificação (dicionário) e integração das bases.
-- Executar com: python src/executar_sql.py src/silver/transformacoes.sql
-- O token ${PROJECT_ID} é substituído pelo executor.
-- ============================================================================

-- 1) Dimensão Município (dados territoriais, chave normalizada)
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.dim_municipio` AS
SELECT DISTINCT
  CAST(id_municipio AS STRING)  AS id_municipio,
  TRIM(nome)                    AS nome_municipio,
  UPPER(TRIM(sigla_uf))         AS sigla_uf,
  TRIM(nome_regiao)             AS nome_regiao
FROM `${PROJECT_ID}.bronze.diretorio_municipio`
WHERE id_municipio IS NOT NULL;

-- 2) Dimensão UF
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.dim_uf` AS
SELECT DISTINCT
  UPPER(TRIM(sigla)) AS sigla_uf,
  TRIM(nome)         AS nome_uf,
  TRIM(regiao)       AS nome_regiao
FROM `${PROJECT_ID}.bronze.diretorio_uf`
WHERE sigla IS NOT NULL;

-- 3) Indicador por Município: decodificação de serie/rede + limpeza de tipos
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.indicador_municipio`
CLUSTER BY id_municipio AS
SELECT
  SAFE_CAST(d.ano AS INT64)                    AS ano,
  CAST(d.id_municipio AS STRING)               AS id_municipio,
  COALESCE(dic_serie.valor, CAST(d.serie AS STRING)) AS serie,
  COALESCE(dic_rede.valor,  CAST(d.rede  AS STRING)) AS rede,
  SAFE_CAST(d.taxa_alfabetizacao AS FLOAT64)   AS taxa_alfabetizacao,
  SAFE_CAST(d.media_portugues    AS FLOAT64)   AS media_portugues
FROM `${PROJECT_ID}.bronze.indicador_municipio` d
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'serie' AND id_tabela = 'municipio'
) dic_serie ON CAST(d.serie AS STRING) = dic_serie.chave
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'rede' AND id_tabela = 'municipio'
) dic_rede ON CAST(d.rede AS STRING) = dic_rede.chave
WHERE d.id_municipio IS NOT NULL
  AND d.taxa_alfabetizacao IS NOT NULL;

-- 4) Indicador por UF: mesma lógica
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.indicador_uf` AS
SELECT
  SAFE_CAST(d.ano AS INT64)                  AS ano,
  UPPER(TRIM(d.sigla_uf))                    AS sigla_uf,
  COALESCE(dic_serie.valor, CAST(d.serie AS STRING)) AS serie,
  COALESCE(dic_rede.valor,  CAST(d.rede  AS STRING)) AS rede,
  SAFE_CAST(d.taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao,
  SAFE_CAST(d.media_portugues    AS FLOAT64) AS media_portugues
FROM `${PROJECT_ID}.bronze.indicador_uf` d
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'serie' AND id_tabela = 'uf'
) dic_serie ON CAST(d.serie AS STRING) = dic_serie.chave
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'rede' AND id_tabela = 'uf'
) dic_rede ON CAST(d.rede AS STRING) = dic_rede.chave
WHERE d.sigla_uf IS NOT NULL
  AND d.taxa_alfabetizacao IS NOT NULL;

-- 5) Metas por Município: de formato largo (uma coluna por ano) para longo
--    (uma linha por ano-meta) — facilita a comparação meta vs resultado na Gold
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.metas_municipio`
CLUSTER BY id_municipio AS
SELECT
  SAFE_CAST(ano AS INT64)              AS ano_base,
  CAST(id_municipio AS STRING)         AS id_municipio,
  CAST(rede AS STRING)                 AS rede,
  SAFE_CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao_base,
  SAFE_CAST(RIGHT(coluna_meta, 4) AS INT64) AS ano_meta,
  SAFE_CAST(valor_meta AS FLOAT64)     AS meta_taxa
FROM `${PROJECT_ID}.bronze.meta_municipio`
UNPIVOT (valor_meta FOR coluna_meta IN (
  meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
  meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
  meta_alfabetizacao_2030))
WHERE id_municipio IS NOT NULL;

-- 6) Metas por UF (longo)
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.metas_uf` AS
SELECT
  SAFE_CAST(ano AS INT64)              AS ano_base,
  UPPER(TRIM(sigla_uf))                AS sigla_uf,
  CAST(rede AS STRING)                 AS rede,
  SAFE_CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao_base,
  SAFE_CAST(RIGHT(coluna_meta, 4) AS INT64) AS ano_meta,
  SAFE_CAST(valor_meta AS FLOAT64)     AS meta_taxa
FROM `${PROJECT_ID}.bronze.meta_uf`
UNPIVOT (valor_meta FOR coluna_meta IN (
  meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
  meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
  meta_alfabetizacao_2030))
WHERE sigla_uf IS NOT NULL;

-- 7) Metas Brasil (longo)
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.metas_brasil` AS
SELECT
  SAFE_CAST(ano AS INT64)              AS ano_base,
  CAST(rede AS STRING)                 AS rede,
  SAFE_CAST(taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao_base,
  SAFE_CAST(RIGHT(coluna_meta, 4) AS INT64) AS ano_meta,
  SAFE_CAST(valor_meta AS FLOAT64)     AS meta_taxa
FROM `${PROJECT_ID}.bronze.meta_brasil`
UNPIVOT (valor_meta FOR coluna_meta IN (
  meta_alfabetizacao_2024, meta_alfabetizacao_2025, meta_alfabetizacao_2026,
  meta_alfabetizacao_2027, meta_alfabetizacao_2028, meta_alfabetizacao_2029,
  meta_alfabetizacao_2030));

-- 8) Alunos (microdados): decodificação completa via dicionário
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.alunos`
CLUSTER BY id_municipio AS
SELECT
  SAFE_CAST(d.ano AS INT64)            AS ano,
  CAST(d.id_municipio AS STRING)       AS id_municipio,
  CAST(d.id_escola AS STRING)          AS id_escola,
  CAST(d.id_aluno AS STRING)           AS id_aluno,
  COALESCE(dic_serie.valor,  CAST(d.serie AS STRING))        AS serie,
  COALESCE(dic_rede.valor,   CAST(d.rede AS STRING))         AS rede,
  COALESCE(dic_pres.valor,   CAST(d.presenca AS STRING))     AS presenca,
  COALESCE(dic_alf.valor,    CAST(d.alfabetizado AS STRING)) AS alfabetizado,
  SAFE_CAST(d.proficiencia AS FLOAT64) AS proficiencia,
  SAFE_CAST(d.peso_aluno   AS FLOAT64) AS peso_aluno
FROM `${PROJECT_ID}.bronze.alunos` d
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'serie' AND id_tabela = 'alunos'
) dic_serie ON CAST(d.serie AS STRING) = dic_serie.chave
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'rede' AND id_tabela = 'alunos'
) dic_rede ON CAST(d.rede AS STRING) = dic_rede.chave
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'presenca' AND id_tabela = 'alunos'
) dic_pres ON CAST(d.presenca AS STRING) = dic_pres.chave
LEFT JOIN (
  SELECT chave, valor FROM `${PROJECT_ID}.bronze.dicionario`
  WHERE nome_coluna = 'alfabetizado' AND id_tabela = 'alunos'
) dic_alf ON CAST(d.alfabetizado AS STRING) = dic_alf.chave
WHERE d.id_aluno IS NOT NULL;

-- 9) Eventos de streaming: deduplicação (fica só o evento mais recente
--    por município+ano) e padronização de tipos
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.eventos_indicador` AS
SELECT
  CAST(id_municipio AS STRING)                    AS id_municipio,
  SAFE_CAST(ano AS INT64)                         AS ano,
  SAFE_CAST(taxa_alfabetizacao AS FLOAT64)        AS taxa_alfabetizacao_evento,
  SAFE_CAST(timestamp_evento AS TIMESTAMP)        AS timestamp_evento
FROM `${PROJECT_ID}.bronze.eventos_indicador`
WHERE id_municipio IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY id_municipio, ano
  ORDER BY SAFE_CAST(timestamp_evento AS TIMESTAMP) DESC
) = 1;

-- 10) INTEGRAÇÃO DAS BASES (requisito da Silver): indicador + território +
--     meta do mesmo ano, uma linha por município/ano/série/rede
CREATE OR REPLACE TABLE `${PROJECT_ID}.silver.indicador_integrado`
CLUSTER BY sigla_uf, id_municipio AS
SELECT
  i.ano,
  i.id_municipio,
  m.nome_municipio,
  m.sigla_uf,
  m.nome_regiao,
  i.serie,
  i.rede,
  i.taxa_alfabetizacao,
  i.media_portugues,
  mm.meta_taxa                       AS meta_taxa_ano,
  CAST(NULL AS FLOAT64)              AS taxa_evento_recente,
  CAST(NULL AS TIMESTAMP)            AS timestamp_evento_recente
FROM `${PROJECT_ID}.silver.indicador_municipio` i
LEFT JOIN `${PROJECT_ID}.silver.dim_municipio` m
  ON i.id_municipio = m.id_municipio
LEFT JOIN `${PROJECT_ID}.silver.metas_municipio` mm
  ON i.id_municipio = mm.id_municipio
 AND i.ano = mm.ano_meta
 AND LOWER(i.rede) = LOWER(mm.rede);

-- 11) MERGE do streaming na base integrada (híbrido batch + streaming
--     convergindo na Silver — requisito do edital)
MERGE `${PROJECT_ID}.silver.indicador_integrado` alvo
USING `${PROJECT_ID}.silver.eventos_indicador` ev
ON  alvo.id_municipio = ev.id_municipio
AND alvo.ano = ev.ano
WHEN MATCHED THEN UPDATE SET
  alvo.taxa_evento_recente      = ev.taxa_alfabetizacao_evento,
  alvo.timestamp_evento_recente = ev.timestamp_evento;
