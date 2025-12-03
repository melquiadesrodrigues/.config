-- ============================================
-- Análise de Logs de Acesso S3 via Athena
-- Conta: 387979423286 | Região: us-east-1
-- ============================================

-- ============================================
-- ETAPA 1: Criar Database
-- ============================================
CREATE DATABASE IF NOT EXISTS s3_logs_analysis;


-- ============================================
-- ETAPA 2: Criar Tabela Particionada
-- ============================================
CREATE EXTERNAL TABLE IF NOT EXISTS s3_logs_analysis.s3_access_logs (
    bucket_owner STRING,
    bucket STRING,
    request_datetime STRING,
    remote_ip STRING,
    requester STRING,
    request_id STRING,
    operation STRING,
    key STRING,
    request_uri STRING,
    http_status STRING,
    error_code STRING,
    bytes_sent BIGINT,
    object_size BIGINT,
    total_time STRING,
    turn_around_time STRING,
    referrer STRING,
    user_agent STRING,
    version_id STRING,
    host_id STRING,
    signature_version STRING,
    cipher_suite STRING,
    authentication_type STRING,
    host_header STRING,
    tls_version STRING,
    access_point_arn STRING,
    acl_required STRING
)
PARTITIONED BY (source_bucket STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1',
    'input.regex' = '([^ ]*) ([^ ]*) \\[(.*)\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\"[^\"]*\"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*).*$'
)
LOCATION 's3://ecs-387979423286-logging-s3/387979423286/';


-- ============================================
-- ETAPA 3: Adicionar Partições
-- ============================================
ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='backuptableau') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/backuptableau/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-default-hml') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-default-hml/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-default-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-default-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-dev') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-dev/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-models-dev') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-models-dev/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-models-hml') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-models-hml/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-models-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-models-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-mvp-data-source-dev') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-mvp-data-source-dev/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-chatbot-trasancional-compass-hml') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-chatbot-trasancional-compass-hml/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-analytics-adjust-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-analytics-adjust-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-databricks-sensitive-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-databricks-sensitive-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-personalization-hub-dev') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-personalization-hub-dev/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-personalization-hub-hml') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-personalization-hub-hml/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-personalization-hub-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-personalization-hub-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-dataops-tableau-server-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-dataops-tableau-server-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='ecs-events-dataops-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/ecs-events-dataops-prd/';

ALTER TABLE s3_logs_analysis.s3_access_logs ADD IF NOT EXISTS 
    PARTITION (source_bucket='s3-ecs-crm-sf-mktcloud-ens-prd') 
    LOCATION 's3://ecs-387979423286-logging-s3/387979423286/s3-ecs-crm-sf-mktcloud-ens-prd/';


-- ============================================
-- ETAPA 4: Query do Relatório (Simples)
-- ============================================
SELECT 
    source_bucket,
    
    -- Última escrita
    MAX(CASE 
        WHEN operation IN ('REST.PUT.OBJECT', 'REST.POST.OBJECT', 'REST.DELETE.OBJECT', 
                          'REST.COPY.OBJECT', 'REST.PUT.PART', 'REST.COMPLETE.UPLOAD',
                          'REST.POST.UPLOADS', 'REST.POST.MULTI_OBJECT_DELETE')
        THEN date_parse(substr(request_datetime, 1, 20), '%d/%b/%Y:%H:%i:%s')
    END) AS ultima_escrita,
    
    -- Última leitura  
    MAX(CASE 
        WHEN operation IN ('REST.GET.OBJECT', 'REST.HEAD.OBJECT')
        THEN date_parse(substr(request_datetime, 1, 20), '%d/%b/%Y:%H:%i:%s')
    END) AS ultima_leitura,
    
    -- Totais
    COUNT(CASE 
        WHEN operation IN ('REST.PUT.OBJECT', 'REST.POST.OBJECT', 'REST.DELETE.OBJECT',
                          'REST.COPY.OBJECT', 'REST.PUT.PART', 'REST.COMPLETE.UPLOAD',
                          'REST.POST.UPLOADS', 'REST.POST.MULTI_OBJECT_DELETE') 
        THEN 1 
    END) AS total_escritas,
    
    COUNT(CASE 
        WHEN operation IN ('REST.GET.OBJECT', 'REST.HEAD.OBJECT') 
        THEN 1 
    END) AS total_leituras,
    
    -- Primeira e última atividade geral
    MIN(date_parse(substr(request_datetime, 1, 20), '%d/%b/%Y:%H:%i:%s')) AS primeira_atividade,
    MAX(date_parse(substr(request_datetime, 1, 20), '%d/%b/%Y:%H:%i:%s')) AS ultima_atividade

FROM s3_logs_analysis.s3_access_logs
WHERE http_status IN ('200', '204', '206')
GROUP BY source_bucket
ORDER BY source_bucket;


-- ============================================
-- ETAPA 5: Query Detalhada (com último objeto)
-- ============================================
WITH ultimas_operacoes AS (
    SELECT 
        source_bucket,
        operation,
        key,
        date_parse(substr(request_datetime, 1, 20), '%d/%b/%Y:%H:%i:%s') AS log_timestamp,
        CASE 
            WHEN operation IN ('REST.PUT.OBJECT', 'REST.POST.OBJECT', 'REST.DELETE.OBJECT',
                              'REST.COPY.OBJECT', 'REST.PUT.PART', 'REST.COMPLETE.UPLOAD',
                              'REST.POST.UPLOADS', 'REST.POST.MULTI_OBJECT_DELETE') 
            THEN 'WRITE'
            WHEN operation IN ('REST.GET.OBJECT', 'REST.HEAD.OBJECT') 
            THEN 'READ'
        END AS op_type
    FROM s3_logs_analysis.s3_access_logs
    WHERE http_status IN ('200', '204', '206')
      AND operation IN (
          'REST.PUT.OBJECT', 'REST.POST.OBJECT', 'REST.DELETE.OBJECT',
          'REST.COPY.OBJECT', 'REST.PUT.PART', 'REST.COMPLETE.UPLOAD',
          'REST.POST.UPLOADS', 'REST.POST.MULTI_OBJECT_DELETE',
          'REST.GET.OBJECT', 'REST.HEAD.OBJECT'
      )
),
ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY source_bucket, op_type ORDER BY log_timestamp DESC) AS rn
    FROM ultimas_operacoes
),
writes AS (
    SELECT source_bucket, log_timestamp, operation, key
    FROM ranked 
    WHERE op_type = 'WRITE' AND rn = 1
),
reads AS (
    SELECT source_bucket, log_timestamp, operation, key
    FROM ranked 
    WHERE op_type = 'READ' AND rn = 1
),
totals AS (
    SELECT 
        source_bucket,
        COUNT(CASE WHEN op_type = 'WRITE' THEN 1 END) AS total_escritas,
        COUNT(CASE WHEN op_type = 'READ' THEN 1 END) AS total_leituras,
        MIN(log_timestamp) AS primeira_atividade,
        MAX(log_timestamp) AS ultima_atividade
    FROM ultimas_operacoes
    GROUP BY source_bucket
)
SELECT 
    COALESCE(t.source_bucket, w.source_bucket, r.source_bucket) AS bucket,
    w.log_timestamp AS ultima_escrita,
    w.operation AS operacao_escrita,
    w.key AS objeto_escrita,
    r.log_timestamp AS ultima_leitura,
    r.operation AS operacao_leitura,
    r.key AS objeto_leitura,
    t.total_escritas,
    t.total_leituras,
    t.primeira_atividade,
    t.ultima_atividade
FROM totals t
LEFT JOIN writes w ON t.source_bucket = w.source_bucket
LEFT JOIN reads r ON t.source_bucket = r.source_bucket
ORDER BY bucket;


-- ============================================
-- LIMPEZA (executar após obter o relatório)
-- ============================================
-- DROP TABLE IF EXISTS s3_logs_analysis.s3_access_logs;
-- DROP DATABASE IF EXISTS s3_logs_analysis;
