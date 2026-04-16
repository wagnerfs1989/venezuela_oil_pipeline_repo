# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Bronze — Vessel Tracking (AISStream via CSV local)
# MAGIC **Projeto:** Venezuela Oil Geopolitics
# MAGIC
# MAGIC **Fluxo:**
# MAGIC 1. Rodar `collect_ais_local.py` na máquina local
# MAGIC 2. Fazer upload do CSV em: Catalog → workspace → bronze_venezuela → ais_uploads
# MAGIC 3. Rodar esse notebook para ingerir no Delta Lake
# MAGIC
# MAGIC **Destino:** `workspace.bronze_venezuela.vessel_tracking` (Delta Lake gerenciado)
# MAGIC
# MAGIC **Validação:** PySpark nativo

# COMMAND ----------

# MAGIC %md ## 1. Imports e Configuração

# COMMAND ----------

import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# ── Configuração Unity Catalog ─────────────────────────────
CATALOG     = "workspace"
DB_BRONZE   = "bronze_venezuela"
TABLE_NAME  = "vessel_tracking"
FULL_TABLE  = f"{CATALOG}.{DB_BRONZE}.{TABLE_NAME}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{DB_BRONZE}/ais_uploads/"

print(f"✅ Configuração carregada")
print(f"🗄️  Tabela destino : {FULL_TABLE}")
print(f"📁 Volume path    : {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md ## 2. Schema e Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DB_BRONZE}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{DB_BRONZE}.ais_uploads")
print(f"✅ Schema e Volume prontos")

# COMMAND ----------

# MAGIC %md ## 3. Lista CSVs disponíveis

# COMMAND ----------

import subprocess

result = subprocess.run(["ls", VOLUME_PATH], capture_output=True, text=True)
files  = [f for f in result.stdout.strip().split("\n") if f.endswith(".csv")]

if files:
    print(f"✅ {len(files)} arquivo(s) encontrado(s):")
    for f in files:
        print(f"   📄 {f}")
else:
    print("⚠️  Nenhum CSV encontrado — faça o upload antes de continuar.")

# COMMAND ----------

# MAGIC %md ## 4. Ingere CSVs no Delta Lake

# COMMAND ----------

df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(f"{VOLUME_PATH}*.csv")

total_raw = df_raw.count()
print(f"📦 Registros lidos: {total_raw:,}")

if total_raw == 0:
    raise ValueError("⚠️  Nenhum dado encontrado. Verifique o upload do CSV.")

df_raw.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(FULL_TABLE)

total = spark.table(FULL_TABLE).count()
print(f"✅ Ingerido | Total acumulado: {total:,}")

# COMMAND ----------

# MAGIC %md ## 5. Validação Bronze — PySpark Nativo

# COMMAND ----------

df_val = spark.table(FULL_TABLE)
total  = df_val.count()
results = []

def check(description, passed, detail=""):
    status = "✅" if passed else "❌"
    msg = f"   {status} {description}"
    if detail: msg += f" — {detail}"
    print(msg)
    results.append((description, passed))

print("🔍 Validações Bronze — Vessel Tracking")
print()

# ── 1. Volume mínimo ───────────────────────────────────────
check("Volume total >= 10 registros", total >= 10, f"{total:,}")

# ── 2. Colunas obrigatórias não nulas ──────────────────────
for c in ["session_id", "message_type", "mmsi", "ingestion_timestamp", "source"]:
    nulls = df_val.filter(col(c).isNull()).count()
    check(f"'{c}' sem nulos", nulls == 0, f"{nulls} nulos")

# ── 3. Tipos de mensagem válidos ───────────────────────────
tipos = [r["message_type"] for r in df_val.select("message_type").distinct().collect()]
tipos_ok = all(t in ["PositionReport", "ShipStaticData"] for t in tipos)
check("message_type válido", tipos_ok, str(tipos))

# ── 4. Coordenadas válidas (PositionReport) ────────────────
df_pos = df_val.filter(col("message_type") == "PositionReport")
pos_count = df_pos.count()

if pos_count > 0:
    lat_min = df_pos.agg({"latitude": "min"}).collect()[0][0]
    lat_max = df_pos.agg({"latitude": "max"}).collect()[0][0]
    lon_min = df_pos.agg({"longitude": "min"}).collect()[0][0]
    lon_max = df_pos.agg({"longitude": "max"}).collect()[0][0]

    check("latitude entre -90 e 90",
        lat_min >= -90 and lat_max <= 90,
        f"range: [{lat_min:.2f}, {lat_max:.2f}]")

    check("longitude entre -180 e 180",
        lon_min >= -180 and lon_max <= 180,
        f"range: [{lon_min:.2f}, {lon_max:.2f}]")

    # Velocidade razoável
    df_speed = df_pos.filter(col("speed").isNotNull())
    if df_speed.count() > 0:
        speed_max = df_speed.agg({"speed": "max"}).collect()[0][0]
        check("speed <= 30 knots", speed_max <= 30, f"máximo: {speed_max}")

# ── 5. Source correto ──────────────────────────────────────
sources = [r["source"] for r in df_val.select("source").distinct().collect()]
check("source = 'AISStream_WebSocket'",
    all(s == "AISStream_WebSocket" for s in sources), str(sources))

# ── 6. Navios únicos ───────────────────────────────────────
navios = df_val.filter((col("ship_name").isNotNull()) & (col("ship_name") != "")) \
    .select("mmsi").distinct().count()
print()
print(f"   🚢 Navios únicos identificados: {navios}")

# ── 7. Sessões de coleta ───────────────────────────────────
sessoes = df_val.select("session_id").distinct().count()
print(f"   📡 Sessões de coleta acumuladas: {sessoes}")

# ── 8. Destinos declarados ─────────────────────────────────
com_destino = df_val.filter(
    (col("message_type") == "ShipStaticData") &
    col("destination").isNotNull() &
    (col("destination") != "")
).count()
print(f"   🗺️  Registros com destino declarado: {com_destino}")

# ── Resultado final ────────────────────────────────────────
print()
passed = sum(1 for _, ok in results if ok)
failed = len(results) - passed

print("=" * 55)
print(f"📊 RESULTADO: {passed}/{len(results)} validações passaram")

if failed > 0:
    print(f"\n❌ {failed} falha(s):")
    for desc, ok in results:
        if not ok:
            print(f"   → {desc}")
    raise ValueError(f"Bronze validation failed: {TABLE_NAME}")
else:
    print(f"✅ Bronze {TABLE_NAME} — aprovado para Silver")
print("=" * 55)
