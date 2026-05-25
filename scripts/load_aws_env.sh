#!/bin/bash
# ============================================================
# Carga credenciales AWS Academy desde .env al shell actual.
# NO toca ~/.aws/credentials — así no se mezcla con las del trabajo.
#
# USO (importante: debe ser sourceado, no ejecutado):
#   source scripts/load_aws_env.sh
#   # o equivalente:
#   . scripts/load_aws_env.sh
#
# Cuando las credenciales expiren (~4h), edita .env y vuelve a sourcearlo.
# ============================================================

# Detectar si se está sourceando o ejecutando
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    echo " Este script DEBE ser sourceado, no ejecutado."
    echo "    Corre:  source scripts/load_aws_env.sh"
    exit 1
fi

# Encontrar el .env (asume que el script está en scripts/)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
ENV_FILE="$REPO_ROOT/.env"

if [ ! -f "$ENV_FILE" ]; then
    echo " No existe $ENV_FILE"
    echo "    Crea uno copiando el ejemplo:"
    echo "      cp .env.example .env"
    echo "    Y pega las credenciales actuales de AWS Academy."
    return 1
fi

# Cargar el .env exportando todas las variables
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

# Verificar que las credenciales estén presentes
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ] || [ -z "$AWS_SESSION_TOKEN" ]; then
    echo " Faltan variables AWS_* en .env"
    return 1
fi

echo " Credenciales AWS cargadas desde .env"
echo "   Region: ${AWS_REGION:-us-east-1}"

# Verificar acceso (silencioso si falla, solo informativo)
if command -v aws > /dev/null 2>&1; then
    if IDENTITY=$(aws sts get-caller-identity --output json 2>/dev/null); then
        ACCOUNT=$(echo "$IDENTITY" | grep -o '"Account": "[^"]*"' | cut -d'"' -f4)
        echo "   Cuenta AWS: $ACCOUNT"
    else
        echo "    No se pudo verificar acceso (¿credenciales expiradas?)"
    fi
fi
