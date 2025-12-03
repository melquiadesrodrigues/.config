#!/bin/bash
#
# Script para análise de logs de acesso S3 via Athena
# Conta: 387979423286 | Região: us-east-1
#

set -e

# Configurações
AWS_REGION="us-east-1"
AWS_ACCOUNT="387979423286"
DATABASE="s3_logs_analysis"
TABLE_NAME="s3_access_logs"
LOGS_BUCKET="ecs-387979423286-logging-s3"
LOGS_PREFIX="387979423286"

# Bucket para resultados do Athena (ajuste se necessário)
ATHENA_OUTPUT="s3://ecs-387979423286-logging-s3/athena-results/"

# Lista de buckets para analisar
BUCKETS=(
    "backuptableau"
    "ecs-chatbot-default-hml"
    "ecs-chatbot-default-prd"
    "ecs-chatbot-dev"
    "ecs-chatbot-models-dev"
    "ecs-chatbot-models-hml"
    "ecs-chatbot-models-prd"
    "ecs-chatbot-mvp-data-source-dev"
    "ecs-chatbot-trasancional-compass-hml"
    "ecs-dataops-analytics-adjust-prd"
    "ecs-dataops-databricks-sensitive-prd"
    "ecs-dataops-personalization-hub-dev"
    "ecs-dataops-personalization-hub-hml"
    "ecs-dataops-personalization-hub-prd"
    "ecs-dataops-tableau-server-prd"
    "ecs-events-dataops-prd"
    "s3-ecs-crm-sf-mktcloud-ens-prd"
)

echo "=============================================="
echo "Análise de Logs S3 via Athena"
echo "=============================================="
echo "Região: $AWS_REGION"
echo "Buckets a analisar: ${#BUCKETS[@]}"
echo "=============================================="

# Função para executar query no Athena e aguardar resultado
run_athena_query() {
    local query="$1"
    local description="$2"
    
    echo ""
    echo ">> $description"
    
    # Iniciar query
    local execution_id=$(aws athena start-query-execution \
        --query-string "$query" \
        --result-configuration "OutputLocation=$ATHENA_OUTPUT" \
        --region $AWS_REGION \
        --output text \
        --query 'QueryExecutionId')
    
    echo "   Query ID: $execution_id"
    
    # Aguardar conclusão
    local status="RUNNING"
    while [[ "$status" == "RUNNING" || "$status" == "QUEUED" ]]; do
        sleep 2
        status=$(aws athena get-query-execution \
            --query-execution-id "$execution_id" \
            --region $AWS_REGION \
            --output text \
            --query 'QueryExecution.Status.State')
        echo -n "."
    done
    echo ""
    
    if [[ "$status" == "SUCCEEDED" ]]; then
        echo "   ✅ Sucesso!"
        return 0
    else
        local reason=$(aws athena get-query-execution \
            --query-execution-id "$execution_id" \
            --region $AWS_REGION \
            --output text \
            --query 'QueryExecution.Status.StateChangeReason')
        echo "   ❌ Falhou: $reason"
        return 1
    fi
}

# 1. Criar database
echo ""
echo "=== ETAPA 1: Criar Database ==="
run_athena_query "CREATE DATABASE IF NOT EXISTS $DATABASE" "Criando database $DATABASE"

# 2. Criar tabela particionada
echo ""
echo "=== ETAPA 2: Criar Tabela Particionada ==="

CREATE_TABLE_SQL="
CREATE EXTERNAL TABLE IF NOT EXISTS $DATABASE.$TABLE_NAME (
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
    'input.regex' = '([^ ]*) ([^ ]*) \\\\[(.*)\\\\] ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\\\"[^\\\"]*\\\"|-) (-|[0-9]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) (\\\"[^\\\"]*\\\"|-) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*).*\$'
)
LOCATION 's3://$LOGS_BUCKET/$LOGS_PREFIX/'
"

run_athena_query "$CREATE_TABLE_SQL" "Criando tabela $TABLE_NAME"

# 3. Adicionar partições para cada bucket
echo ""
echo "=== ETAPA 3: Adicionar Partições ==="

for bucket in "${BUCKETS[@]}"; do
    ADD_PARTITION_SQL="
    ALTER TABLE $DATABASE.$TABLE_NAME 
    ADD IF NOT EXISTS PARTITION (source_bucket='$bucket') 
    LOCATION 's3://$LOGS_BUCKET/$LOGS_PREFIX/$bucket/'
    "
    run_athena_query "$ADD_PARTITION_SQL" "Adicionando partição: $bucket"
done

# 4. Executar query do relatório
echo ""
echo "=== ETAPA 4: Gerar Relatório ==="

REPORT_SQL="
SELECT 
    source_bucket,
    bucket,
    
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

FROM $DATABASE.$TABLE_NAME
WHERE http_status IN ('200', '204', '206')
GROUP BY source_bucket, bucket
ORDER BY source_bucket
"

echo ""
echo ">> Executando relatório final..."

EXECUTION_ID=$(aws athena start-query-execution \
    --query-string "$REPORT_SQL" \
    --result-configuration "OutputLocation=$ATHENA_OUTPUT" \
    --region $AWS_REGION \
    --output text \
    --query 'QueryExecutionId')

echo "   Query ID: $EXECUTION_ID"

# Aguardar conclusão
STATUS="RUNNING"
while [[ "$STATUS" == "RUNNING" || "$STATUS" == "QUEUED" ]]; do
    sleep 5
    STATUS=$(aws athena get-query-execution \
        --query-execution-id "$EXECUTION_ID" \
        --region $AWS_REGION \
        --output text \
        --query 'QueryExecution.Status.State')
    
    # Mostrar progresso
    SCANNED=$(aws athena get-query-execution \
        --query-execution-id "$EXECUTION_ID" \
        --region $AWS_REGION \
        --output text \
        --query 'QueryExecution.Statistics.DataScannedInBytes' 2>/dev/null || echo "0")
    
    SCANNED_MB=$((${SCANNED:-0} / 1024 / 1024))
    echo "   Status: $STATUS | Dados escaneados: ${SCANNED_MB} MB"
done

if [[ "$STATUS" == "SUCCEEDED" ]]; then
    echo ""
    echo "✅ Relatório gerado com sucesso!"
    echo ""
    
    # Mostrar estatísticas
    STATS=$(aws athena get-query-execution \
        --query-execution-id "$EXECUTION_ID" \
        --region $AWS_REGION \
        --query 'QueryExecution.Statistics')
    
    echo "Estatísticas da query:"
    echo "$STATS" | jq -r '"  Tempo de execução: \(.EngineExecutionTimeInMillis // 0 | . / 1000) segundos\n  Dados escaneados: \(.DataScannedInBytes // 0 | . / 1024 / 1024 | floor) MB\n  Custo estimado: $\(.DataScannedInBytes // 0 | . / 1024 / 1024 / 1024 / 1024 * 5 | . * 100 | floor / 100)"'
    
    # Baixar resultado
    RESULT_FILE="s3_access_report_$(date +%Y%m%d_%H%M%S).csv"
    
    echo ""
    echo "Baixando resultado para: $RESULT_FILE"
    aws s3 cp "${ATHENA_OUTPUT}${EXECUTION_ID}.csv" "./$RESULT_FILE" --region $AWS_REGION
    
    echo ""
    echo "=============================================="
    echo "RESULTADO DO RELATÓRIO"
    echo "=============================================="
    column -t -s',' "$RESULT_FILE" | head -50
    
    echo ""
    echo "=============================================="
    echo "Arquivo CSV salvo em: ./$RESULT_FILE"
    echo "=============================================="
    
else
    REASON=$(aws athena get-query-execution \
        --query-execution-id "$EXECUTION_ID" \
        --region $AWS_REGION \
        --output text \
        --query 'QueryExecution.Status.StateChangeReason')
    echo "❌ Falhou: $REASON"
    exit 1
fi
