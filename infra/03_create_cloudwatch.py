#!/usr/bin/env python3
"""
ShopStream — Infra Paso 3: CloudWatch Dashboard + Alarma
==========================================================

Crea (idempotente):
  - Dashboard "ShopStream-Ingesta" con widgets de:
      • Archivos recibidos vs válidos vs cuarentena
      • Registros válidos vs inválidos
      • Tamaño promedio de archivos
      • Errores de lectura
      • Logs recientes de la Lambda
  - Alarma "ShopStream-ErroresLectura" que dispara si hay >5 errores en 5 min

Uso:
    python infra/03_create_cloudwatch.py
    python infra/03_create_cloudwatch.py --teardown
"""
import argparse
import json
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    AWS_REGION,
    CLOUDWATCH_NAMESPACE,
    CLOUDWATCH_DASHBOARD,
    LAMBDA_FUNCTION_NAME,
)

ALARM_NAME = "ShopStream-ErroresLectura"


def build_dashboard_body() -> str:
    body = {
        "widgets": [
            {
                "type": "metric",
                "x": 0, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "title": "Archivos: Recibidos / Válidos / Cuarentena",
                    "view": "timeSeries",
                    "stacked": False,
                    "region": AWS_REGION,
                    "metrics": [
                        [CLOUDWATCH_NAMESPACE, "ArchivosRecibidos", "Pipeline", "ShopStream"],
                        [".", "ArchivosValidos", ".", "."],
                        [".", "ArchivosEnCuarentena", ".", "."],
                    ],
                    "period": 300,
                    "stat": "Sum",
                },
            },
            {
                "type": "metric",
                "x": 12, "y": 0, "width": 12, "height": 6,
                "properties": {
                    "title": "Registros: Válidos vs Inválidos",
                    "view": "timeSeries",
                    "stacked": True,
                    "region": AWS_REGION,
                    "metrics": [
                        [CLOUDWATCH_NAMESPACE, "RegistrosValidos", "Pipeline", "ShopStream"],
                        [".", "RegistrosInvalidos", ".", "."],
                    ],
                    "period": 300,
                    "stat": "Sum",
                },
            },
            {
                "type": "metric",
                "x": 0, "y": 6, "width": 12, "height": 6,
                "properties": {
                    "title": "Tamaño de archivos (Bytes)",
                    "view": "timeSeries",
                    "stacked": False,
                    "region": AWS_REGION,
                    "metrics": [
                        [CLOUDWATCH_NAMESPACE, "TamanoArchivo", "Pipeline", "ShopStream",
                         {"stat": "Average"}],
                        ["...", {"stat": "Maximum"}],
                    ],
                    "period": 300,
                },
            },
            {
                "type": "metric",
                "x": 12, "y": 6, "width": 12, "height": 6,
                "properties": {
                    "title": "Errores de lectura",
                    "view": "singleValue",
                    "region": AWS_REGION,
                    "metrics": [
                        [CLOUDWATCH_NAMESPACE, "ErroresLectura", "Pipeline", "ShopStream"],
                    ],
                    "period": 300,
                    "stat": "Sum",
                    "sparkline": True,
                },
            },
            {
                "type": "log",
                "x": 0, "y": 12, "width": 24, "height": 6,
                "properties": {
                    "title": "Logs recientes de Lambda",
                    "region": AWS_REGION,
                    "query": (
                        f"SOURCE '/aws/lambda/{LAMBDA_FUNCTION_NAME}'\n"
                        "| fields @timestamp, @message\n"
                        "| sort @timestamp desc\n"
                        "| limit 50"
                    ),
                    "view": "table",
                },
            },
        ]
    }
    return json.dumps(body)


def create_dashboard() -> None:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    cw.put_dashboard(
        DashboardName=CLOUDWATCH_DASHBOARD,
        DashboardBody=build_dashboard_body(),
    )
    print(f"  ✓ Dashboard creado/actualizado: {CLOUDWATCH_DASHBOARD}")
    print(f"     URL: https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home?region={AWS_REGION}#dashboards:name={CLOUDWATCH_DASHBOARD}")


def create_alarm() -> None:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    cw.put_metric_alarm(
        AlarmName=ALARM_NAME,
        AlarmDescription="Más de 5 errores de lectura en 5 minutos en la Lambda de ingesta",
        ActionsEnabled=True,
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricName="ErroresLectura",
        Dimensions=[{"Name": "Pipeline", "Value": "ShopStream"}],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=5,
        ComparisonOperator="GreaterThanThreshold",
        TreatMissingData="notBreaching",
    )
    print(f"  ✓ Alarma creada: {ALARM_NAME}")


def delete_dashboard() -> None:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    try:
        cw.delete_dashboards(DashboardNames=[CLOUDWATCH_DASHBOARD])
        print(f"  ✓ Dashboard eliminado: {CLOUDWATCH_DASHBOARD}")
    except ClientError as e:
        print(f"  · Dashboard skip: {e}")


def delete_alarm() -> None:
    cw = boto3.client("cloudwatch", region_name=AWS_REGION)
    try:
        cw.delete_alarms(AlarmNames=[ALARM_NAME])
        print(f"  ✓ Alarma eliminada: {ALARM_NAME}")
    except ClientError as e:
        print(f"  · Alarma skip: {e}")


def verify_identity() -> None:
    sts = boto3.client("sts", region_name=AWS_REGION)
    try:
        sts.get_caller_identity()
    except Exception as e:
        print(f"✗ Credenciales AWS inválidas: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--teardown", action="store_true")
    args = parser.parse_args()
    verify_identity()

    print("=" * 60)
    print(" ShopStream — CloudWatch")
    print("=" * 60)

    if args.teardown:
        delete_alarm()
        delete_dashboard()
    else:
        create_dashboard()
        create_alarm()

    print("=" * 60)


if __name__ == "__main__":
    main()
