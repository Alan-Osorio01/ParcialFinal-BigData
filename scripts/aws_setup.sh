#!/bin/bash
# ============================================================
# AWS Academy — Helper para configurar credenciales
# ============================================================
# Uso:
#   1. Inicia el lab en AWS Academy
#   2. Click en "AWS Details" → muestra las credenciales de la sesión
#   3. Copia el bloque [default] que aparece
#   4. Ejecuta este script y pega las credenciales cuando lo pida
#
# Las credenciales de Academy expiran cuando termina la sesión (~4h).
# Volver a correr este script cuando expiren.
# ============================================================

set -e

AWS_DIR="$HOME/.aws"
mkdir -p "$AWS_DIR"

echo ""
echo "============================================"
echo " AWS Academy — Setup de Credenciales"
echo "============================================"
echo ""
echo "En la consola Academy, click 'AWS Details' y copia el bloque:"
echo ""
echo "  [default]"
echo "  aws_access_key_id=ASIA..."
echo "  aws_secret_access_key=..."
echo "  aws_session_token=..."
echo ""
echo "Pega el contenido completo (luego presiona Ctrl+D para terminar):"
echo ""

cat > "$AWS_DIR/credentials"

# Configurar región por defecto
cat > "$AWS_DIR/config" <<EOF
[default]
region = us-east-1
output = json
EOF

echo ""
echo "Credenciales guardadas en $AWS_DIR/credentials"
echo "Region por defecto: us-east-1"
echo ""

# Verificar
echo "Verificando acceso a AWS..."
if aws sts get-caller-identity > /dev/null 2>&1; then
    IDENTITY=$(aws sts get-caller-identity --output json)
    echo " Acceso OK"
    echo "$IDENTITY"
else
    echo " Error de acceso. Verifica que pegaste bien las credenciales."
    exit 1
fi

echo ""
echo "Listo. Ya puedes correr los scripts de infra/"
