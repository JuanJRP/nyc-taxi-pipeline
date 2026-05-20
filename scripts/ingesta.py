"""
=============================================================
ETAPA 1: INGESTA — Descarga datos reales TLC NYC por año
=============================================================
Fuente: https://d37ci6vzurychx.cloudfront.net/trip-data/
Formato: Yellow Taxi Trip Records (.parquet mensual)

Configurable por variable de entorno YEARS (ej: "2024,2025,2026")
=============================================================
"""

import os, time, urllib.request, urllib.error

BASE_DIR    = "/app"
RAW_DIR     = os.path.join(BASE_DIR, "data", "raw")
BASE_URL    = "https://d37ci6vzurychx.cloudfront.net/trip-data"

os.makedirs(RAW_DIR, exist_ok=True)


def descargar_mes(year: int, month: int) -> str | None:
    nombre = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url    = f"{BASE_URL}/{nombre}"
    dest   = os.path.join(RAW_DIR, nombre)

    if os.path.exists(dest) and os.path.getsize(dest) > 1_000_000:
        mb = os.path.getsize(dest) / 1024**2
        print(f"{nombre} ya existe ({mb:.0f} MB)")
        return dest

    print(f"{nombre} ...", flush=True)
    t0 = time.time()
    try:
        def hook(b, bs, total):
            if total > 0 and b % 300 == 0:
                pct = min(b * bs / total * 100, 100)
                print(f"      {pct:4.0f}%", end="\r", flush=True)

        urllib.request.urlretrieve(url, dest, hook)
        mb = os.path.getsize(dest) / 1024**2
        print(f"{nombre}  {mb:.0f} MB  ({time.time()-t0:.0f}s)      ")
        return dest
    except Exception as e:
        print(f"{nombre}: {e}")
        if os.path.exists(dest):
            os.remove(dest)
        return None


def run():
    years_str = os.environ.get("YEARS", "2023")
    years     = [int(y.strip()) for y in years_str.split(",")]

    print(f"\n  Descargando años: {years}")
    print(f"  Destino: {RAW_DIR}\n")

    total, ok = 0, 0
    for year in years:
        print(f"\n Año {year}:")
        for month in range(1, 13):
            total += 1
            if descargar_mes(year, month):
                ok += 1

    print(f"\n Descarga completada: {ok}/{total} archivos en {RAW_DIR}")
    if ok == 0:
        print(" No se descargó ningún archivo. Verifica conexión a internet.")
        raise SystemExit(1)
