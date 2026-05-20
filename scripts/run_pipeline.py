"""
=============================================================
NYC TAXI PIPELINE — ORQUESTADOR PRINCIPAL
=============================================================
Ejecuta las 3 etapas en secuencia:
  1. Ingesta   → descarga Parquets reales del TLC NYC por año
  2. Spark     → limpieza, features, agregaciones, MongoDB
  3. Visualización → 6 gráficas profesionales

Variables de entorno:
  MONGO_URI  → URI de conexión a MongoDB
  YEARS      → años a procesar, separados por coma (ej: "2022,2025")
=============================================================
"""

import os, sys, time

def banner(title: str):
    print("\n" + "═" * 65)
    print(f"  {title}")
    print("═" * 65)

t_total = time.time()

banner("NYC TAXI DATA PIPELINE — INICIO")
print(f"  Dominio           : Movilidad urbana — Taxis amarillos NYC (TLC)")
print(f"  Años configurados : {os.environ.get('YEARS', '2023,2024')}")
print(f"  MongoDB URI       : {os.environ.get('MONGO_URI', 'no configurado')}")
print()
print("  PROBLEMA DE NEGOCIO:")
print("  La Comisión de Taxi y Limusinas de NYC necesita entender los patrones")
print("  de demanda y rentabilidad del servicio para optimizar la distribución")
print("  de licencias, tarifas y operaciones.")
print()
print("  PREGUNTAS ANALÍTICAS:")
print("  1. ¿En qué horas y días se concentra la mayor demanda de viajes?")
print("  2. ¿Cómo evoluciona el revenue mensual? ¿Existe estacionalidad?")
print("  3. ¿Qué zonas de pickup generan más viajes e ingresos?")
print("  4. ¿Qué método de pago predomina y cómo afecta la propina promedio?")
print("  5. ¿Cuál es la relación entre duración del viaje y propinas?")
print("  6. ¿Cómo varían tarifa y velocidad promedio según temporada?")

# ── Etapa 1: Ingesta ─────────────────────────────────────────
banner("ETAPA 1 / 3 — INGESTA DE DATOS REALES")
import ingesta
ingesta.run()

# ── Etapa 2: Procesamiento Spark ─────────────────────────────
banner("ETAPA 2 / 3 — PROCESAMIENTO CON PYSPARK")
import procesamiento_spark
procesamiento_spark.run()

# ── Etapa 3: Visualización ───────────────────────────────────
banner("ETAPA 3 / 3 — GENERACIÓN DE VISUALIZACIONES")
import visualizacion
visualizacion.run()

# ── Resumen ──────────────────────────────────────────────────
banner("PIPELINE COMPLETADO")
print(f"  Tiempo total: {(time.time() - t_total)/60:.1f} minutos")
print(f"  Gráficas en : /app/charts/")
print(f"  MongoDB     : nyc_taxi (colecciones: agg_by_hour, agg_by_day,")
print(f"                           agg_by_zone, agg_payment, pipeline_summary)")
print("═" * 65)
