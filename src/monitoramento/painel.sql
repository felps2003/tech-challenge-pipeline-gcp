-- ============================================================================
-- PAINEL DE OBSERVABILIDADE — Tech Challenge Fase 2
-- Consultas sobre os logs de governança (o executor imprime os resultados).
-- Executar com: python src/executar_sql.py src/monitoramento/painel.sql
-- ============================================================================

-- Latência: duração média e máxima por etapa nas execuções registradas
SELECT etapa,
       COUNT(*)                        AS execucoes,
       ROUND(AVG(duracao_segundos),1)  AS duracao_media_s,
       ROUND(MAX(duracao_segundos),1)  AS duracao_maxima_s,
       COUNTIF(status = 'FALHA')       AS falhas
FROM `${PROJECT_ID}.governanca.log_execucoes`
GROUP BY etapa
ORDER BY duracao_media_s DESC;

-- Qualidade: histórico de regras que já dispararam ALERTA ou FALHA
SELECT DATE(data_execucao) AS dia, regra, tabela, status, registros_afetados
FROM `${PROJECT_ID}.governanca.log_qualidade`
WHERE status != 'OK'
ORDER BY data_execucao DESC
LIMIT 20;

-- Saúde geral: última execução do pipeline, etapa a etapa
SELECT etapa, status, duracao_segundos
FROM `${PROJECT_ID}.governanca.log_execucoes`
WHERE id_execucao = (
  SELECT MAX(id_execucao) FROM `${PROJECT_ID}.governanca.log_execucoes`
)
ORDER BY inicio;
