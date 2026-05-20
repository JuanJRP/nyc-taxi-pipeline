"""
=============================================================
ETAPA 2: PROCESAMIENTO CON PYSPARK
=============================================================
- Normalización de tipos via pandas (staging) → evita bug de
  Spark 3.5 con tipos mixtos INT64/DOUBLE del TLC NYC
- Limpieza y validación con reglas de negocio del dominio
- Feature engineering temporal y operativo
- 5 agregaciones analíticas clave
- Exportación a MongoDB + Parquet particionado
=============================================================
"""

import os, time, json, glob
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

BASE_DIR     = "/app"
RAW_DIR      = os.path.join(BASE_DIR, "data", "raw")
STAGING_DIR  = os.path.join(BASE_DIR, "data", "staging")
PROC_PATH    = os.path.join(BASE_DIR, "data", "processed", "nyc_taxi_clean.parquet")
OUTPUT_PATH  = os.path.join(BASE_DIR, "data", "output")
MONGO_URI    = os.environ.get("MONGO_URI",
               "mongodb://admin:taxi2025@mongodb:27017/nyc_taxi?authSource=admin")

for d in [STAGING_DIR, os.path.dirname(PROC_PATH), OUTPUT_PATH]:
    os.makedirs(d, exist_ok=True)

# Columnas y sus tipos objetivo
COLS = [
    "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime",
    "passenger_count","trip_distance","RatecodeID","store_and_fwd_flag",
    "PULocationID","DOLocationID","payment_type","fare_amount","extra",
    "mta_tax","tip_amount","tolls_amount","improvement_surcharge",
    "total_amount","congestion_surcharge","airport_fee",
]
FLOAT_COLS = [
    "passenger_count","trip_distance","RatecodeID","fare_amount","extra",
    "mta_tax","tip_amount","tolls_amount","improvement_surcharge",
    "total_amount","congestion_surcharge","airport_fee",
]
INT_COLS = ["VendorID","PULocationID","DOLocationID","payment_type"]


def normalizar_parquets():
    """
    Paso previo: pandas lee cada Parquet raw, unifica tipos y lo guarda
    en staging/. Así Spark no encuentra conflictos de tipos.
    Se ejecuta solo una vez por archivo (idempotente).
    """
    raw_files = sorted(glob.glob(os.path.join(RAW_DIR, "yellow_tripdata_*.parquet")))
    if not raw_files:
        raise FileNotFoundError(f"No hay archivos en {RAW_DIR}. Ejecuta ingesta primero.")

    staged = []
    for path in raw_files:
        name = os.path.basename(path)
        out  = os.path.join(STAGING_DIR, name)
        staged.append(out)
        if os.path.exists(out) and os.path.getsize(out) > 100_000:
            print(f"   ⏭️  staging/{name} ya normalizado")
            continue
        print(f"   🔧 Normalizando {name}...", end=" ", flush=True)
        pdf = pd.read_parquet(path)
        for c in COLS:
            if c not in pdf.columns:
                pdf[c] = None
        pdf = pdf[COLS].copy()
        for c in FLOAT_COLS:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype("float64")
        for c in INT_COLS:
            pdf[c] = pd.to_numeric(pdf[c], errors="coerce").astype("Int64")
        for tc in ["tpep_pickup_datetime","tpep_dropoff_datetime"]:
            pdf[tc] = pd.to_datetime(pdf[tc], errors="coerce").astype("datetime64[us]")
        pdf.to_parquet(out, index=False, engine="pyarrow", compression="snappy")
        print(f"{len(pdf):,} filas")
        del pdf
    return staged


def run():
    t0 = time.time()

    # ── Paso 0: normalizar tipos en staging ─────────────────────
    print("\n🔧 Normalizando tipos de datos...")
    staged = normalizar_parquets()
    print(f"   {len(staged)} archivos listos en staging/\n")

    # ── Spark Session ────────────────────────────────────────────
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Pipeline")

        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")

        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")

        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.default.parallelism", "12")

        .config("spark.memory.fraction", "0.7")
        .config("spark.memory.storageFraction", "0.3")

        .getOrCreate()
        )
    spark.sparkContext.setLogLevel("ERROR")
    print(f"✅ Spark {spark.version} | {spark.sparkContext.defaultParallelism} cores")

    # ── Etapa 1: Carga ───────────────────────────────────────────
    print("\n📥 Cargando datos desde staging...")
    df = spark.read.parquet(*staged)
    n_raw = df.count()
    print(f"   Registros cargados: {n_raw:,}")

    # ── Etapa 2: Limpieza ────────────────────────────────────────
    print("\n🧹 Limpieza y validación...")
    df_clean = (df
        .dropna(subset=["tpep_pickup_datetime","tpep_dropoff_datetime",
                        "trip_distance","fare_amount","total_amount"])
        .filter(F.col("fare_amount").between(2.5, 500))
        .filter(F.col("trip_distance").between(0.1, 100))
        .filter(F.col("passenger_count").between(1, 6))
        .filter(F.col("total_amount") > 0)
        .filter(F.col("tip_amount") >= 0)
        .filter(F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime"))
        .filter((F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) < 14400)
        .filter(F.year("tpep_pickup_datetime").between(2020, 2026))
        .filter(F.col("RatecodeID").between(1, 6))
    )
    n_clean = df_clean.count()
    print(f"   Válidos  : {n_clean:>10,} ({n_clean/n_raw*100:.1f}%)")
    print(f"   Filtrados: {n_raw-n_clean:>10,} ({(n_raw-n_clean)/n_raw*100:.1f}%)")

    # ── Etapa 3: Feature Engineering ────────────────────────────
    print("\n⚙️  Feature Engineering...")
    df_feat = (df_clean
        .withColumn("pickup_year",      F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month",     F.month("tpep_pickup_datetime"))
        .withColumn("pickup_hour",      F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("pickup_date",      F.to_date("tpep_pickup_datetime"))
        .withColumn("is_rush_hour",
            F.when(F.col("pickup_hour").between(7, 9),   "morning_rush")
             .when(F.col("pickup_hour").between(16, 19), "evening_rush")
             .when(F.col("pickup_hour").between(0, 5),   "overnight")
             .otherwise("normal"))
        .withColumn("is_weekend",
            F.when(F.col("pickup_dayofweek").isin([1, 7]), True).otherwise(False))
        .withColumn("season",
            F.when(F.col("pickup_month").isin([12, 1, 2]),  "Invierno")
             .when(F.col("pickup_month").isin([3, 4, 5]),   "Primavera")
             .when(F.col("pickup_month").isin([6, 7, 8]),   "Verano")
             .otherwise("Otoño"))
        .withColumn("trip_duration_min",
            F.round((F.unix_timestamp("tpep_dropoff_datetime") -
                     F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2))
        .withColumn("avg_speed_mph",
            F.when(F.col("trip_duration_min") > 0,
                   F.round(F.col("trip_distance") / (F.col("trip_duration_min")/60.0), 2))
             .otherwise(None))
        .withColumn("tip_ratio",
            F.when(F.col("fare_amount") > 0,
                   F.round(F.col("tip_amount") / F.col("fare_amount"), 4))
             .otherwise(0.0))
        .withColumn("payment_label",
            F.when(F.col("payment_type") == 1, "Credit Card")
             .when(F.col("payment_type") == 2, "Cash")
             .when(F.col("payment_type") == 3, "No Charge")
             .otherwise("Other"))
        .withColumn("day_name",
            F.when(F.col("pickup_dayofweek") == 1, "Domingo")
             .when(F.col("pickup_dayofweek") == 2, "Lunes")
             .when(F.col("pickup_dayofweek") == 3, "Martes")
             .when(F.col("pickup_dayofweek") == 4, "Miércoles")
             .when(F.col("pickup_dayofweek") == 5, "Jueves")
             .when(F.col("pickup_dayofweek") == 6, "Viernes")
             .otherwise("Sábado"))
    )
    df_feat.persist(StorageLevel.MEMORY_AND_DISK)
    print("   Features: year, month, hour, dayofweek, rush_hour, weekend, season,")
    print("             duration, speed, tip_ratio, payment_label, day_name")

    # ── Etapa 4: Guardar Parquet procesado ──────────────────────
    print(f"\n💾 Guardando Parquet procesado (particionado por año/mes)...")
    (df_feat
        .repartition(48, "pickup_year", "pickup_month")
        .write.mode("overwrite")
        .partitionBy("pickup_year", "pickup_month")
        .parquet(PROC_PATH))
    print(f"   ✅ {PROC_PATH}")

    # ── Etapa 5: Agregaciones ────────────────────────────────────
    print("\n📊 Calculando agregaciones...")

    df_feat.unpersist()

    df_aggs = spark.read.parquet(PROC_PATH)

    agg_hour = (df_aggs
        .groupBy("pickup_hour", "is_rush_hour")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
            F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        ).orderBy("pickup_hour"))

    agg_day = (df_aggs
        .groupBy("pickup_dayofweek", "day_name", "is_weekend")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
        ).orderBy("pickup_dayofweek"))

    agg_zone = (df_aggs
        .groupBy("PULocationID")
        .agg(
            F.count("*").alias("total_pickups"),
            F.round(F.avg("total_amount"), 2).alias("avg_revenue_per_trip"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
        ).orderBy(F.desc("total_pickups")).limit(20))

    agg_payment = (df_aggs
        .groupBy("payment_label")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
            F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
        ).orderBy(F.desc("total_trips")))

    agg_monthly = (df_aggs
        .groupBy("pickup_year", "pickup_month", "season")
        .agg(
            F.count("*").alias("total_trips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
        ).orderBy("pickup_year", "pickup_month"))

    # Guardar CSVs
    for agg, name in [
        (agg_hour,    "agg_by_hour"),
        (agg_day,     "agg_by_day"),
        (agg_zone,    "agg_by_zone"),
        (agg_payment, "agg_payment"),
        (agg_monthly, "agg_monthly"),
    ]:
        agg.coalesce(1).write.mode("overwrite").option("header", True).csv(
            os.path.join(OUTPUT_PATH, name))
        print(f"   ✅ {name}")

    # ── Etapa 6: Resumen global ──────────────────────────────────
    row = df_feat.agg(
        F.count("*").alias("total_trips"),
        F.round(F.sum("total_amount"), 0).alias("total_revenue_usd"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("tip_ratio")*100, 2).alias("avg_tip_pct"),
        F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed_mph"),
        F.min("tpep_pickup_datetime").alias("first_trip"),
        F.max("tpep_pickup_datetime").alias("last_trip"),
    ).collect()[0]

    summary = {
        "total_trips":        int(row["total_trips"]),
        "total_revenue_usd":  float(row["total_revenue_usd"]),
        "avg_fare":           float(row["avg_fare"]),
        "avg_distance_miles": float(row["avg_distance_miles"]),
        "avg_duration_min":   float(row["avg_duration_min"]),
        "avg_speed_mph":      float(row["avg_speed_mph"]),
        "avg_tip_pct":        float(row["avg_tip_pct"]),
        "processing_time_s":  round(time.time()-t0, 1),
        "years":              os.environ.get("YEARS", "2025"),
    }
    with open(os.path.join(OUTPUT_PATH, "pipeline_summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    # ── Etapa 7: Exportar a MongoDB ──────────────────────────────
    print("\n🍃 Exportando a MongoDB...")
    try:
        def to_mongo(df_agg, collection):
            (df_agg.write.format("mongodb").mode("overwrite")
                .option("database", "nyc_taxi")
                .option("collection", collection)
                .save())
            print(f"   ✅ MongoDB ← {collection}")

        to_mongo(agg_hour,    "agg_by_hour")
        to_mongo(agg_day,     "agg_by_day")
        to_mongo(agg_zone,    "agg_by_zone")
        to_mongo(agg_payment, "agg_payment")
        to_mongo(agg_monthly, "agg_monthly")

        # Guardar summary en Mongo también
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI)
        db = client["nyc_taxi"]
        db["pipeline_summary"].replace_one({}, summary, upsert=True)
        client.close()
        print("   ✅ MongoDB ← pipeline_summary")

    except Exception as e:
        print(f"   ⚠️  MongoDB no disponible: {e}")

    # ── Resumen final ────────────────────────────────────────────
    elapsed = time.time() - t0
    print(f"""
{'='*65}
  PROCESAMIENTO COMPLETADO
{'='*65}
  📊 Viajes procesados  : {summary['total_trips']:>12,}
  💰 Revenue total      : ${summary['total_revenue_usd']:>12,.0f}
  💵 Tarifa promedio    : ${summary['avg_fare']:>11.2f}
  📍 Distancia prom.    : {summary['avg_distance_miles']:>10.2f} mi
  ⏱️  Duración prom.     : {summary['avg_duration_min']:>10.2f} min
  🚗 Velocidad prom.    : {summary['avg_speed_mph']:>10.2f} mph
  💳 Propina prom.      : {summary['avg_tip_pct']:>10.2f} %
  📅 Período            : {row['first_trip']} → {row['last_trip']}
  ⏱️  Tiempo Spark       : {elapsed:.0f}s
{'='*65}""")

    spark.stop()
