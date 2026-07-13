-- ============================================================================
-- QUALIDADE DE DADOS — Tech Challenge Fase 2
-- Implementa as 4 regras exigidas pelo edital:
--   1. Verificação de duplicidade
--   2. Detecção de valores ausentes
--   3. Validação de chaves de relacionamento
--   4. Consistência entre tabelas
-- Cada regra grava o resultado em governanca.log_qualidade (governança).
-- Executar com: python src/executar_sql.py src/qualidade/validacoes.sql
-- ============================================================================

-- 0) Tabela de log (criada uma vez, acumula o histórico das execuções)
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.governanca.log_qualidade` (
  data_execucao       TIMESTAMP,
  regra               STRING,
  tabela              STRING,
  status              STRING,
  registros_afetados  INT64,
  detalhe             STRING
);

-- 1a) DUPLICIDADE — indicador por município (chave: ano+município+série+rede)
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'duplicidade', 'silver.indicador_municipio',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'chaves ano+id_municipio+serie+rede com mais de 1 registro'
FROM (
  SELECT ano, id_municipio, serie, rede
  FROM `${PROJECT_ID}.silver.indicador_municipio`
  GROUP BY ano, id_municipio, serie, rede
  HAVING COUNT(*) > 1
);

-- 1b) DUPLICIDADE — alunos (chave: ano+id_aluno)
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'duplicidade', 'silver.alunos',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'chaves ano+id_aluno com mais de 1 registro'
FROM (
  SELECT ano, id_aluno
  FROM `${PROJECT_ID}.silver.alunos`
  GROUP BY ano, id_aluno
  HAVING COUNT(*) > 1
);

-- 2a) VALORES AUSENTES — colunas críticas do indicador integrado
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'valores_ausentes', 'silver.indicador_integrado',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'linhas sem taxa_alfabetizacao ou sem id_municipio'
FROM `${PROJECT_ID}.silver.indicador_integrado`
WHERE taxa_alfabetizacao IS NULL OR id_municipio IS NULL;

-- 2b) VALORES AUSENTES — municípios sem nome (falha de join com o diretório)
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'valores_ausentes', 'silver.indicador_integrado',
       IF(COUNT(*) = 0, 'OK', 'ALERTA'), COUNT(*),
       'linhas sem nome_municipio (id sem correspondencia no diretorio)'
FROM `${PROJECT_ID}.silver.indicador_integrado`
WHERE nome_municipio IS NULL;

-- 3a) CHAVES DE RELACIONAMENTO — todo município do indicador existe na dimensão?
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'chave_relacionamento',
       'silver.indicador_municipio -> dim_municipio',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'id_municipio do indicador sem correspondencia na dim_municipio'
FROM `${PROJECT_ID}.silver.indicador_municipio` i
LEFT JOIN `${PROJECT_ID}.silver.dim_municipio` m USING (id_municipio)
WHERE m.id_municipio IS NULL;

-- 3b) CHAVES DE RELACIONAMENTO — eventos de streaming apontam para municípios válidos?
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'chave_relacionamento',
       'silver.eventos_indicador -> dim_municipio',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'id_municipio de evento sem correspondencia na dim_municipio'
FROM `${PROJECT_ID}.silver.eventos_indicador` e
LEFT JOIN `${PROJECT_ID}.silver.dim_municipio` m USING (id_municipio)
WHERE m.id_municipio IS NULL;

-- 4a) CONSISTÊNCIA — taxa de alfabetização deve estar entre 0 e 100
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'consistencia', 'silver.indicador_integrado',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'taxa_alfabetizacao fora do intervalo [0, 100]'
FROM `${PROJECT_ID}.silver.indicador_integrado`
WHERE taxa_alfabetizacao < 0 OR taxa_alfabetizacao > 100;

-- 4b) CONSISTÊNCIA — metas devem estar entre 0 e 100
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'consistencia', 'silver.metas_municipio',
       IF(COUNT(*) = 0, 'OK', 'FALHA'), COUNT(*),
       'meta_taxa fora do intervalo [0, 100]'
FROM `${PROJECT_ID}.silver.metas_municipio`
WHERE meta_taxa < 0 OR meta_taxa > 100;

-- 4c) CONSISTÊNCIA — número de municípios distintos compatível com o Brasil (~5.570)
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'consistencia', 'silver.dim_municipio',
       IF(COUNT(DISTINCT id_municipio) BETWEEN 5500 AND 5600, 'OK', 'ALERTA'),
       COUNT(DISTINCT id_municipio),
       'total de municipios distintos na dimensao (esperado ~5.570)'
FROM `${PROJECT_ID}.silver.dim_municipio`;

-- 4d) CONSISTÊNCIA — agregado municipal condiz com o agregado por UF?
--     (media das taxas municipais nao deve divergir absurdamente da taxa da UF)
INSERT INTO `${PROJECT_ID}.governanca.log_qualidade`
SELECT CURRENT_TIMESTAMP(), 'consistencia',
       'silver.indicador_municipio vs silver.indicador_uf',
       IF(COUNT(*) = 0, 'OK', 'ALERTA'), COUNT(*),
       'UFs cuja media municipal diverge mais de 20 pontos da taxa oficial da UF'
FROM (
  SELECT m.sigla_uf
  FROM (
    SELECT dm.sigla_uf, i.ano, i.serie, i.rede, AVG(i.taxa_alfabetizacao) AS media_municipal
    FROM `${PROJECT_ID}.silver.indicador_municipio` i
    JOIN `${PROJECT_ID}.silver.dim_municipio` dm USING (id_municipio)
    GROUP BY dm.sigla_uf, i.ano, i.serie, i.rede
  ) m
  JOIN `${PROJECT_ID}.silver.indicador_uf` u
    ON  m.sigla_uf = u.sigla_uf AND m.ano = u.ano
    AND m.serie = u.serie AND m.rede = u.rede
  WHERE ABS(m.media_municipal - u.taxa_alfabetizacao) > 20
);

-- 5) RELATÓRIO — resultado da execução mais recente
SELECT regra, tabela, status, registros_afetados, detalhe
FROM `${PROJECT_ID}.governanca.log_qualidade`
WHERE data_execucao = (SELECT MAX(data_execucao) FROM `${PROJECT_ID}.governanca.log_qualidade`)
   OR data_execucao >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE)
ORDER BY status DESC, regra;
