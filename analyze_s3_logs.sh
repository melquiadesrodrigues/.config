#!/bin/bash
#
# Wrapper para executar o analisador de logs S3
# Conta: 387979423286 | Região: us-east-1
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/s3_logs_analyzer.py"

echo "Verificando dependências..."

# Verificar Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 não encontrado. Por favor, instale Python 3."
    exit 1
fi

echo "✅ Python 3 encontrado: $(python3 --version)"

# Verificar boto3
if ! python3 -c "import boto3" 2>/dev/null; then
    echo ""
    echo "⚠️  boto3 não está instalado"
    echo "Instalando boto3..."
    pip3 install boto3 --user
fi

echo "✅ boto3 disponível"
echo ""

# Verificar credenciais AWS
if ! aws sts get-caller-identity &>/dev/null; then
    echo "❌ Credenciais AWS não configuradas ou inválidas"
    echo "Configure com: aws configure"
    exit 1
fi

ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "✅ AWS configurado - Conta: $ACCOUNT"
echo ""

# Tornar o script Python executável
chmod +x "$PYTHON_SCRIPT"

# Executar o script Python
echo "Iniciando análise de logs..."
echo ""

python3 "$PYTHON_SCRIPT" "$@"
