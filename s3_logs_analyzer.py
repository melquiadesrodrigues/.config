#!/usr/bin/env python3
"""
Analisador de Logs de Acesso S3
Conta: 387979423286 | Região: us-east-1

Este script lê logs diretamente do S3 e gera relatório de última leitura/escrita
sem necessidade de Athena ou Glue.
"""

import boto3
import re
import gzip
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurações
AWS_REGION = "us-east-1"
LOGS_BUCKET = "ecs-387979423286-logging-s3"
LOGS_PREFIX = "387979423286"

# Lista de buckets para analisar
TARGET_BUCKETS = [
    "backuptableau",
    "ecs-chatbot-default-hml",
    "ecs-chatbot-default-prd",
    "ecs-chatbot-dev",
    "ecs-chatbot-models-dev",
    "ecs-chatbot-models-hml",
    "ecs-chatbot-models-prd",
    "ecs-chatbot-mvp-data-source-dev",
    "ecs-chatbot-trasancional-compass-hml",
    "ecs-dataops-analytics-adjust-prd",
    "ecs-dataops-databricks-sensitive-prd",
    "ecs-dataops-personalization-hub-dev",
    "ecs-dataops-personalization-hub-hml",
    "ecs-dataops-personalization-hub-prd",
    "ecs-dataops-tableau-server-prd",
    "ecs-events-dataops-prd",
    "s3-ecs-crm-sf-mktcloud-ens-prd",
]

# Regex para parsear logs S3
# Formato: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
LOG_PATTERN = re.compile(
    r'(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) '
    r'"([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
    r'"([^"]*)" (\S+)(?: (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) (\S+))?'
)

# Operações de escrita
WRITE_OPERATIONS = {
    'REST.PUT.OBJECT',
    'REST.POST.OBJECT',
    'REST.DELETE.OBJECT',
    'REST.COPY.OBJECT',
    'REST.PUT.PART',
    'REST.COMPLETE.UPLOAD',
    'REST.POST.UPLOADS',
    'REST.POST.MULTI_OBJECT_DELETE',
    'BATCH.DELETE.OBJECT',
}

# Operações de leitura
READ_OPERATIONS = {
    'REST.GET.OBJECT',
    'REST.HEAD.OBJECT',
}

# Status HTTP de sucesso
SUCCESS_STATUS = {'200', '204', '206'}


class BucketStats:
    """Estatísticas de um bucket"""
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.ultima_escrita: Optional[datetime] = None
        self.ultima_leitura: Optional[datetime] = None
        self.total_escritas = 0
        self.total_leituras = 0
        self.primeira_atividade: Optional[datetime] = None
        self.ultima_atividade: Optional[datetime] = None
        self.ultimo_objeto_escrito: Optional[str] = None
        self.ultimo_objeto_lido: Optional[str] = None


def parse_log_line(line: str) -> Optional[Dict]:
    """Parseia uma linha de log S3"""
    match = LOG_PATTERN.match(line)
    if not match:
        return None

    groups = match.groups()

    # Parse datetime
    try:
        timestamp_str = groups[2]  # [06/Nov/2024:10:15:23 +0000]
        timestamp = datetime.strptime(timestamp_str, '%d/%b/%Y:%H:%M:%S %z')
    except (ValueError, IndexError):
        return None

    return {
        'bucket_owner': groups[0],
        'bucket': groups[1],
        'timestamp': timestamp,
        'remote_ip': groups[3],
        'requester': groups[4],
        'request_id': groups[5],
        'operation': groups[6],
        'key': groups[7],
        'request_uri': groups[8],
        'http_status': groups[9],
        'error_code': groups[10],
    }


def process_log_file(s3_client, bucket: str, key: str, stats: Dict[str, BucketStats]) -> int:
    """Processa um arquivo de log"""
    lines_processed = 0

    try:
        # Baixar arquivo
        response = s3_client.get_object(Bucket=bucket, Key=key)

        # Descompactar se for gzip
        if key.endswith('.gz'):
            content = gzip.decompress(response['Body'].read()).decode('utf-8', errors='ignore')
        else:
            content = response['Body'].read().decode('utf-8', errors='ignore')

        # Processar cada linha
        for line in content.splitlines():
            if not line.strip():
                continue

            log_entry = parse_log_line(line)
            if not log_entry:
                continue

            # Filtrar apenas status de sucesso
            if log_entry['http_status'] not in SUCCESS_STATUS:
                continue

            bucket_name = log_entry['bucket']
            operation = log_entry['operation']
            timestamp = log_entry['timestamp']
            key_name = log_entry['key']

            # Inicializar stats se necessário
            if bucket_name not in stats:
                stats[bucket_name] = BucketStats(bucket_name)

            bucket_stats = stats[bucket_name]

            # Atualizar primeira/última atividade
            if bucket_stats.primeira_atividade is None or timestamp < bucket_stats.primeira_atividade:
                bucket_stats.primeira_atividade = timestamp
            if bucket_stats.ultima_atividade is None or timestamp > bucket_stats.ultima_atividade:
                bucket_stats.ultima_atividade = timestamp

            # Processar operações de escrita
            if operation in WRITE_OPERATIONS:
                bucket_stats.total_escritas += 1
                if bucket_stats.ultima_escrita is None or timestamp > bucket_stats.ultima_escrita:
                    bucket_stats.ultima_escrita = timestamp
                    bucket_stats.ultimo_objeto_escrito = key_name

            # Processar operações de leitura
            elif operation in READ_OPERATIONS:
                bucket_stats.total_leituras += 1
                if bucket_stats.ultima_leitura is None or timestamp > bucket_stats.ultima_leitura:
                    bucket_stats.ultima_leitura = timestamp
                    bucket_stats.ultimo_objeto_lido = key_name

            lines_processed += 1

    except Exception as e:
        print(f"  ⚠️  Erro ao processar {key}: {e}", file=sys.stderr)

    return lines_processed


def analyze_bucket_logs(bucket_name: str, max_workers: int = 10) -> Dict[str, BucketStats]:
    """Analisa logs de um bucket específico"""
    print(f"\n📦 Analisando logs: {bucket_name}")

    s3_client = boto3.client('s3', region_name=AWS_REGION)
    stats = {}

    # Listar todos os arquivos de log para este bucket
    prefix = f"{LOGS_PREFIX}/{bucket_name}/"

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        log_files = []

        print(f"   Listando arquivos de log...")
        for page in paginator.paginate(Bucket=LOGS_BUCKET, Prefix=prefix):
            if 'Contents' in page:
                log_files.extend([obj['Key'] for obj in page['Contents']])

        if not log_files:
            print(f"   ⚠️  Nenhum arquivo de log encontrado")
            return stats

        print(f"   Encontrados {len(log_files)} arquivos de log")
        print(f"   Processando com {max_workers} threads...")

        total_lines = 0
        files_processed = 0

        # Processar arquivos em paralelo
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(process_log_file, s3_client, LOGS_BUCKET, log_file, stats): log_file
                for log_file in log_files
            }

            for future in as_completed(futures):
                log_file = futures[future]
                try:
                    lines = future.result()
                    total_lines += lines
                    files_processed += 1

                    if files_processed % 100 == 0:
                        print(f"   Processados {files_processed}/{len(log_files)} arquivos, {total_lines} linhas...")

                except Exception as e:
                    print(f"   ⚠️  Erro em {log_file}: {e}", file=sys.stderr)

        print(f"   ✅ Total: {files_processed} arquivos, {total_lines} linhas processadas")

    except Exception as e:
        print(f"   ❌ Erro ao listar logs: {e}", file=sys.stderr)

    return stats


def main():
    print("=" * 60)
    print("Analisador de Logs de Acesso S3")
    print("=" * 60)
    print(f"Região: {AWS_REGION}")
    print(f"Bucket de Logs: {LOGS_BUCKET}")
    print(f"Buckets a analisar: {len(TARGET_BUCKETS)}")
    print("=" * 60)

    all_stats = {}

    # Analisar cada bucket
    for bucket_name in TARGET_BUCKETS:
        bucket_stats = analyze_bucket_logs(bucket_name)
        all_stats.update(bucket_stats)

    # Gerar relatório
    print("\n" + "=" * 60)
    print("RELATÓRIO DE ATIVIDADE DOS BUCKETS")
    print("=" * 60)

    if not all_stats:
        print("Nenhuma atividade encontrada nos logs")
        return

    # Salvar em CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = f"s3_access_report_{timestamp}.csv"

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'bucket',
            'ultima_escrita',
            'ultimo_objeto_escrito',
            'ultima_leitura',
            'ultimo_objeto_lido',
            'total_escritas',
            'total_leituras',
            'primeira_atividade',
            'ultima_atividade'
        ])

        # Ordenar por nome do bucket
        for bucket_name in sorted(all_stats.keys()):
            stats = all_stats[bucket_name]
            writer.writerow([
                bucket_name,
                stats.ultima_escrita.isoformat() if stats.ultima_escrita else '',
                stats.ultimo_objeto_escrito or '',
                stats.ultima_leitura.isoformat() if stats.ultima_leitura else '',
                stats.ultimo_objeto_lido or '',
                stats.total_escritas,
                stats.total_leituras,
                stats.primeira_atividade.isoformat() if stats.primeira_atividade else '',
                stats.ultima_atividade.isoformat() if stats.ultima_atividade else '',
            ])

    # Exibir resumo
    print("\nResumo por bucket:")
    print("-" * 60)

    for bucket_name in sorted(all_stats.keys()):
        stats = all_stats[bucket_name]
        print(f"\n📦 {bucket_name}")
        print(f"   Última escrita: {stats.ultima_escrita or 'N/A'}")
        if stats.ultimo_objeto_escrito:
            print(f"   Objeto: {stats.ultimo_objeto_escrito[:80]}...")
        print(f"   Última leitura: {stats.ultima_leitura or 'N/A'}")
        if stats.ultimo_objeto_lido:
            print(f"   Objeto: {stats.ultimo_objeto_lido[:80]}...")
        print(f"   Total escritas: {stats.total_escritas}")
        print(f"   Total leituras: {stats.total_leituras}")

    print("\n" + "=" * 60)
    print(f"✅ Relatório salvo em: {csv_file}")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrompido pelo usuário")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Erro fatal: {e}", file=sys.stderr)
        sys.exit(1)
