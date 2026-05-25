#!/bin/bash
# ============================================================
#  ATENCION: Este script fue REEMPLAZADO.
# ============================================================
# El método anterior escribía en ~/.aws/credentials y pisaba
# las credenciales del trabajo. Ahora usamos .env aislado.
#
# Pasos nuevos:
#   1. cp .env.example .env
#   2. Edita .env y pega las credenciales actuales de AWS Academy
#   3. En CADA terminal donde vayas a usar AWS, corre:
#        source scripts/load_aws_env.sh
#
# Tus credenciales del trabajo en ~/.aws/credentials NO se tocan.
# ============================================================

echo ""
echo "  Este script ya NO se usa."
echo ""
echo "   Usa en su lugar:"
echo ""
echo "      1) cp .env.example .env       # solo la primera vez"
echo "      2) nano .env                  # pega tus credenciales Academy"
echo "      3) source scripts/load_aws_env.sh"
echo ""
echo "    Tus credenciales del trabajo (~/.aws/credentials) NO se tocan."
echo ""
exit 1
