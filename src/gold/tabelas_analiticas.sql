-- ============================================================================
-- CAMADA GOLD
-- Datasets analíticos prontos para dashboards, estatística e ML:
--   1. Indicador de alfabetização por município
--   2. Comparação entre metas e resultados
--   3. Evolução temporal do indicador
-- + bônus: resumo de proficiência por município a partir dos microdados
-- Executar com: python src/executar_sql.py src/gold/tabelas_analiticas.sql
-- ============================================================================

-- 1) INDICADOR POR MUNICÍPIO — foto mais recente de cada município,
--    com território e o último evento de streaming incorporado
CREATE OR REPLACE TABLE `${PROJECT_ID}.gold.indicador_por_municipio`
CLUSTER BY sigla_uf AS
SELECT
  ano,
  id_municipio,
  nome_municipio,
  sigla_uf,
  nome_regiao,
  serie,
  rede,
  taxa_alfabetizacao,
  media_portugues,
  taxa_evento_recente,
  timestamp_evento_recente
FROM `${PROJECT_ID}.silver.indicador_integrado`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY id_municipio, serie, rede
  ORDER BY ano DESC
) = 1;

-- 2) META VS RESULTADO — nos três níveis (município, UF e Brasil),
--    com gap em pontos e flag de atingimento
CREATE OR REPLACE TABLE `${PROJECT_ID}.gold.meta_vs_resultado`
CLUSTER BY nivel, sigla_uf AS

-- nível município (a meta do mesmo ano já está integrada na Silver)
SELECT
  'municipio'                          AS nivel,
  i.ano,
  i.id_municipio                       AS chave,
  i.nome_municipio                     AS nome,
  i.sigla_uf,
  i.rede,
  i.taxa_alfabetizacao                 AS resultado,
  i.meta_taxa_ano                      AS meta,
  ROUND(i.taxa_alfabetizacao - i.meta_taxa_ano, 2) AS gap_pontos,
  i.taxa_alfabetizacao >= i.meta_taxa_ano          AS atingiu_meta
FROM `${PROJECT_ID}.silver.indicador_integrado` i
WHERE i.meta_taxa_ano IS NOT NULL

UNION ALL

-- nível UF
SELECT
  'uf', u.ano, u.sigla_uf, du.nome_uf, u.sigla_uf, u.rede,
  u.taxa_alfabetizacao,
  mu.meta_taxa,
  ROUND(u.taxa_alfabetizacao - mu.meta_taxa, 2),
  u.taxa_alfabetizacao >= mu.meta_taxa
FROM `${PROJECT_ID}.silver.indicador_uf` u
JOIN `${PROJECT_ID}.silver.metas_uf` mu
  ON u.sigla_uf = mu.sigla_uf
 AND u.ano = mu.ano_meta
 AND LOWER(u.rede) = LOWER(mu.rede)
LEFT JOIN `${PROJECT_ID}.silver.dim_uf` du ON u.sigla_uf = du.sigla_uf

UNION ALL

-- nível Brasil (taxa observada no ano-base comparada à meta daquele ano)
SELECT
  'brasil', mb.ano_base, 'BR', 'Brasil', CAST(NULL AS STRING), mb.rede,
  mb.taxa_alfabetizacao_base,
  mb.meta_taxa,
  ROUND(mb.taxa_alfabetizacao_base - mb.meta_taxa, 2),
  mb.taxa_alfabetizacao_base >= mb.meta_taxa
FROM `${PROJECT_ID}.silver.metas_brasil` mb
WHERE mb.ano_meta = mb.ano_base;

-- 3) EVOLUÇÃO TEMPORAL — série do indicador por ano, nos três níveis
CREATE OR REPLACE TABLE `${PROJECT_ID}.gold.evolucao_temporal`
CLUSTER BY nivel AS
SELECT
  'municipio' AS nivel,
  id_municipio AS chave,
  nome_municipio AS nome,
  sigla_uf,
  ano, serie, rede,
  taxa_alfabetizacao
FROM `${PROJECT_ID}.silver.indicador_integrado`

UNION ALL

SELECT 'uf', u.sigla_uf, du.nome_uf, u.sigla_uf, u.ano, u.serie, u.rede, u.taxa_alfabetizacao
FROM `${PROJECT_ID}.silver.indicador_uf` u
LEFT JOIN `${PROJECT_ID}.silver.dim_uf` du ON u.sigla_uf = du.sigla_uf

UNION ALL

SELECT DISTINCT 'brasil', 'BR', 'Brasil', CAST(NULL AS STRING),
       ano_base, CAST(NULL AS STRING), rede, taxa_alfabetizacao_base
FROM `${PROJECT_ID}.silver.metas_brasil`;

-- 4) BÔNUS — resumo de proficiência por município a partir dos MICRODADOS,
--    usando o ponto de corte oficial de 743 pontos (Pesquisa Alfabetiza Brasil)
CREATE OR REPLACE TABLE `${PROJECT_ID}.gold.proficiencia_municipio`
CLUSTER BY sigla_uf AS
SELECT
  a.ano,
  a.id_municipio,
  m.nome_municipio,
  m.sigla_uf,
  a.rede,
  COUNT(*)                                   AS alunos_avaliados,
  ROUND(AVG(a.proficiencia), 1)              AS proficiencia_media,
  ROUND(100 * SAFE_DIVIDE(
      SUM(IF(a.proficiencia >= 743, a.peso_aluno, 0)),
      SUM(IF(a.proficiencia IS NOT NULL, a.peso_aluno, 0))), 2)
                                             AS pct_acima_corte_743
FROM `${PROJECT_ID}.silver.alunos` a
LEFT JOIN `${PROJECT_ID}.silver.dim_municipio` m USING (id_municipio)
WHERE a.proficiencia IS NOT NULL
GROUP BY a.ano, a.id_municipio, m.nome_municipio, m.sigla_uf, a.rede;

-- ============================================================================
-- AMOSTRAS (impressas no terminal pelo executor)
-- ============================================================================

-- Os 10 municípios mais distantes da meta (onde a política pública é mais urgente)
SELECT nome, sigla_uf, rede, ano, resultado, meta, gap_pontos
FROM `${PROJECT_ID}.gold.meta_vs_resultado`
WHERE nivel = 'municipio' AND gap_pontos IS NOT NULL
ORDER BY gap_pontos ASC
LIMIT 10;

-- Evolução do Brasil
SELECT ano, rede, taxa_alfabetizacao
FROM `${PROJECT_ID}.gold.evolucao_temporal`
WHERE nivel = 'brasil'
ORDER BY ano, rede;
