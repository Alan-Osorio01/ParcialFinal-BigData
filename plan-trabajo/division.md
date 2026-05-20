# ShopStream — División de Trabajo Parcial III

## Integrantes

| Archivo | Persona | Rol |
|---|---|---|
| [alan.md](alan.md) | Alan | Fundación del pipeline: datos sintéticos + ingesta Lambda + CI/CD |
| [daniela.md](daniela.md) | Daniela | Procesamiento distribuido: PySpark + EMR + métricas + anomalías |
| [ana.md](ana.md) | Ana | Datawarehouse + Glue Studio + API REST con Zappa |

---

## Cuenta AWS Academy — Reglas Compartidas

> Todos trabajan en la misma cuenta Academy. Respetar las convenciones de nombres para evitar conflictos.

- **Región fija:** `us-east-1`
- **IAM Role:** usar `LabRole` (no se puede crear roles nuevos)
- **Sesiones:** expiran ~4 horas. Guardar configuraciones como código en GitHub antes de que expire.
- **NO borrar** recursos del otro sin avisar en el chat del grupo.

### Convención de nombres de recursos AWS

```
shopstream-<recurso>-<iniciales>
```

Ejemplos:
- S3 raw:          `shopstream-raw-aad`
- S3 processed:    `shopstream-processed-aad`
- S3 quarantine:   `shopstream-quarantine-aad`
- Lambda ingesta:  `shopstream-ingesta-validator`
- EMR cluster:     `shopstream-emr-cluster`
- RDS:             `shopstream-rds-dw`
- Glue DB:         `shopstream_glue_db`
- API Gateway:     `shopstream-api`

---

## Árbol de Dependencias

```
[Alan] Setup repo + S3 buckets + datos sintéticos
           │
           ├──► [Alan]   Lambda de validación + CloudWatch
           │
           ├──► [Daniela] EMR + PySpark pipeline (necesita datos en S3 raw)
           │
           └──► [Ana]    RDS schema + Glue (adelantar diseño)
                              │
                  [Daniela termina Parquet en S3 processed]
                              │
                              └──► [Ana] Glue ETL S3→RDS + API funcional
```

---

## Cronograma General

| Día | Alan | Daniela | Ana |
|---|---|---|---|
| 1 | Setup repo + AWS + schemas | Setup entorno EMR | Setup RDS + DW schema |
| 2 | Generador de datos + S3 upload | Limpieza PySpark (datos muestra de Alan) | Adelantar Glue Studio visual |
| 3 | Lambda validación + quarantine | Métricas (1-3) | Triggers Glue + Data Quality |
| 4 | CloudWatch + CI/CD GitHub Actions | Métricas (4-6) + Anomalías | API Flask + Zappa setup |
| 5 | Tests unitarios Alan + integración | Exportar Parquet + notebook/script | 3 endpoints + API Gateway |
| 6 | Revisión conjunta + PR reviews | Tests PySpark + documentación EMR | Tests API + integración RDS |
| 7 | Test end-to-end + fixes | Test end-to-end + fixes | Test end-to-end + fixes |
| 8 | **Entrega final** | **Entrega final** | **Entrega final** |

---

## Sincronizaciones Obligatorias (reuniones cortas)

1. **Día 1 (inicio):** Setup conjunto de repo y acceso AWS Academy
2. **Día 2 (Alan → todos):** Alan sube primer dataset a S3 → Daniela y Ana pueden comenzar
3. **Día 5 (Daniela → Ana):** Daniela entrega Parquet en S3 processed → Ana activa Glue real
4. **Día 7 (todos):** Test end-to-end del pipeline completo
5. **Día 8 (todos):** Revisión final y push a GitHub

---

## Estructura del Repositorio

```
ParcialFinal-BigData/
├── README.md
├── division.md          ← este archivo (índice)
├── alan.md              ← tareas detalladas Alan
├── daniela.md           ← tareas detalladas Daniela
├── ana.md               ← tareas detalladas Ana
├── .github/
│   └── workflows/
│       └── ci.yml       ← Alan configura
├── infra/               ← Alan configura
│   ├── s3_setup.sh
│   ├── rds_init.sql
│   └── emr_cluster.json
├── punto1_datos/        ← Alan
├── punto2_ingesta/      ← Alan
├── punto3_pyspark/      ← Daniela
├── punto4_glue/         ← Ana
├── api/                 ← Ana
└── docs/
    └── screenshots/
```
