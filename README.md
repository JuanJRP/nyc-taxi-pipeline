# NYC Taxi Data Pipeline — Open Source

[![CI](https://github.com/JuanJRP/nyc-taxi-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/JuanJRP/nyc-taxi-pipeline/actions/workflows/ci.yml)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-7-47A248?logo=mongodb&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-compose-2496ED?logo=docker&logoColor=white)

Pipeline de datos completo para analizar los viajes de taxis amarillos de Nueva York,
construido íntegramente con herramientas open-source sobre Docker.

![Dashboard ejecutivo generado por el pipeline](charts/06_dashboard_ejecutivo.png)

---

## 1. Dominio y caso de uso real

**Dominio:** Movilidad urbana — Transporte público de Nueva York  
**Fuente de datos:** [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — datos reales y públicos en formato Parquet mensual.

---

## 2. Problema de negocio

La Comisión de Taxi y Limusinas de Nueva York (TLC) necesita comprender los patrones
de **demanda, rentabilidad y comportamiento operativo** del servicio de taxis amarillos
para tomar decisiones basadas en datos sobre:

- Optimización de la distribución de licencias por zona y franja horaria.
- Ajuste de tarifas según demanda estacional y horaria.
- Identificación de ineficiencias operativas (velocidad, duración, ocupación).
- Comprensión del comportamiento del pasajero según método de pago.

---

## 3. Preguntas analíticas

| # | Pregunta | Agregación que la responde |
|---|----------|---------------------------|
| 1 | ¿En qué horas y días de la semana se concentra la mayor demanda de viajes? | `agg_by_hour`, `agg_by_day` |
| 2 | ¿Cómo evoluciona el revenue mensual? ¿Existe estacionalidad marcada? | `agg_monthly` |
| 3 | ¿Qué zonas de pickup generan más viajes e ingresos por viaje? | `agg_by_zone` |
| 4 | ¿Qué método de pago predomina y cómo influye en la propina promedio? | `agg_payment` |
| 5 | ¿Cuál es la relación entre duración del viaje, hora del día y generosidad en propinas? | `agg_by_hour` |
| 6 | ¿Cómo varían la tarifa y la velocidad promedio según la temporada del año? | `agg_monthly` |

---

## 4. Arquitectura del pipeline

```
┌─────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  TLC NYC    │────▶│  INGESTA (batch) │────▶│  PROCESAMIENTO   │────▶│  VISUALIZACIÓN  │
│  CloudFront │     │  ingesta.py      │     │  PySpark 3.5     │────▶│  visualizacion  │
│  (.parquet) │     │  urllib descarga │     │  procesamiento_  │     │  .py            │
└─────────────┘     │  mensual por año │     │  spark.py        │     │  6 gráficas PNG │
                    └──────────────────┘     └────────┬─────────┘     └─────────────────┘
                                                      │
                                          ┌───────────┴──────────┐
                                          │                      │
                                   ┌──────▼──────┐       ┌──────▼──────┐
                                   │   Parquet   │       │   MongoDB   │
                                   │  particionado│      │  nyc_taxi   │
                                   │ (año/mes)   │       │  (NoSQL)    │
                                   └─────────────┘       └─────────────┘
```

### Decisión de diseño: Batch vs Streaming

El pipeline usa **procesamiento batch** (no streaming) de forma deliberada:

- **Naturaleza de los datos:** El TLC publica datos históricos mensuales consolidados,
  no hay un flujo en tiempo real disponible públicamente.
- **Volumen por ejecución:** Cada año contiene ~12 archivos Parquet de ~50–100 MB cada uno,
  lo que hace que el batch sea la estrategia óptima para este caso.
- **Reproducibilidad:** El batch garantiza resultados idénticos en cada ejecución sobre el
  mismo conjunto de datos, lo que es esencial para análisis regulatorios.

Para un escenario de producción con datos en tiempo real (ej. GPS de vehículos activos),
la capa de ingesta podría reemplazarse por **Apache Kafka + Spark Structured Streaming**
sin modificar las etapas de procesamiento y almacenamiento.

---

## 5. Stack tecnológico (100 % open-source)

| Componente | Herramienta | Versión | Rol |
|---|---|---|---|
| Ingesta | Python `urllib` | 3.11 | Descarga batch desde URL pública |
| Procesamiento distribuido | **Apache Spark / PySpark** | 3.5.3 | Limpieza, features, agregaciones |
| Staging | **Apache Parquet** + pyarrow | 17.0 | Formato columnar de alto rendimiento |
| Almacenamiento NoSQL | **MongoDB** | 6.0 | Persistencia de agregados analíticos |
| Visualización | matplotlib + seaborn | 3.9 / 0.13 | 6 gráficas profesionales |
| Orquestación | **Docker** + Docker Compose | 3.8 | Reproducibilidad total |

---

## 6. Estructura del proyecto

```
├── Dockerfile                  # Imagen Python 3.11 + Java 17 + dependencias
├── docker-compose.yml          # Stack completo: MongoDB + pipeline
├── requirements.txt            # Dependencias Python
├── scripts/
│   ├── run_pipeline.py         # Orquestador principal (etapas 1→2→3)
│   ├── ingesta.py              # Etapa 1: descarga Parquets TLC por año/mes
│   ├── procesamiento_spark.py  # Etapa 2: normalización, limpieza, features, agregaciones
│   └── visualizacion.py        # Etapa 3: 6 visualizaciones PNG
├── charts/                     # Gráficas generadas (montado desde el host)
└── README.md
```

---

## 7. Etapas del pipeline

### Etapa 1 — Ingesta (ingesta.py)
- Descarga archivos Parquet mensuales reales desde `d37ci6vzurychx.cloudfront.net`
- Configurable por variable de entorno `YEARS` (ej: `"2023,2024"`)
- Idempotente: omite archivos ya descargados
- Almacena en `/app/data/raw/`

### Etapa 2 — Procesamiento PySpark (procesamiento_spark.py)
1. **Normalización de tipos** con pandas (staging) → evita conflictos INT64/DOUBLE de Spark 3.5
2. **Limpieza y validación** con reglas de negocio del dominio TLC
3. **Feature engineering**: hora, día, temporada, rush hour, duración, velocidad, tip_ratio
4. **Persistencia** en Parquet particionado por año/mes
5. **5 agregaciones analíticas**: por hora, día, zona, pago, mes
6. **Exportación a MongoDB** via mongo-spark-connector 10.3

### Etapa 3 — Visualización (visualizacion.py)
| Archivo | Visualización |
|---|---|
| `01_heatmap_demanda.png` | Mapa de calor hora × día de la semana |
| `02_evolucion_mensual.png` | Revenue y volumen mensual con tendencia |
| `03_top20_zonas.png` | Top 20 zonas por pickups con KPIs inline |
| `04_metodos_pago.png` | Donut + comparativa tarifa/propina por pago |
| `05_analisis_hora.png` | Volumen por hora + scatter duración vs propina |
| `06_dashboard_ejecutivo.png` | Dashboard con KPIs + 3 minigráficas |

---

## 8. Cómo ejecutar

### Prerequisitos
- Docker Engine ≥ 24
- Docker Compose ≥ 2
- ~5 GB de espacio en disco por año de datos

### Inicio rápido

```bash
# Clonar / copiar los archivos y ubicarse en la raíz del proyecto
docker compose up --build
```

El pipeline se ejecuta automáticamente en secuencia. Las gráficas quedan en `./charts/`.

### Configurar años a procesar

```bash
# Procesar solo 2024
YEARS=2024 docker compose up --build

# Procesar 2022, 2023 y 2024
YEARS="2022,2023,2024" docker compose up --build
```

> **Nota:** Solo usar años ≤ año actual con datos publicados por el TLC.
> Los meses futuros simplemente no existen en la fuente y se omiten.

### Ver datos en MongoDB

```bash
docker exec -it mongodb mongosh \
  "mongodb://admin:taxi2025@localhost:27017/nyc_taxi?authSource=admin"

# En el shell de Mongo:
db.agg_by_hour.find().limit(3)
db.pipeline_summary.findOne()
```

---

## 9. Esquema de datos — colecciones MongoDB

| Colección | Descripción | Campos clave |
|---|---|---|
| `agg_by_hour` | Agregados por hora del día | `pickup_hour`, `total_trips`, `avg_fare`, `avg_tip_pct` |
| `agg_by_day` | Agregados por día de semana | `day_name`, `is_weekend`, `total_revenue` |
| `agg_by_zone` | Top 20 zonas por pickups | `PULocationID`, `total_pickups`, `avg_revenue_per_trip` |
| `agg_payment` | Análisis por método de pago | `payment_label`, `avg_tip`, `avg_tip_pct` |
| `agg_monthly` | Evolución mensual | `pickup_year`, `pickup_month`, `season`, `avg_speed_mph` |
| `pipeline_summary` | KPIs globales del run | `total_trips`, `total_revenue_usd`, `processing_time_s` |

---

## 10. Limpieza y reglas de validación aplicadas

| Regla | Criterio |
|---|---|
| Tarifa base | Entre $2.50 y $500 |
| Distancia | Entre 0.1 y 100 millas |
| Pasajeros | Entre 1 y 6 |
| Duración | Menos de 4 horas |
| Propina | ≥ 0 |
| Año válido | Entre 2020 y 2026 |
| RatecodeID | Entre 1 y 6 (códigos TLC válidos) |
| Timestamps | pickup < dropoff (sin viajes negativos) |

---

*Proyecto académico — Institución Universitaria Pascual Bravo — Big Data Pipeline Open Source*# Pepiline_Taxis_NYC
