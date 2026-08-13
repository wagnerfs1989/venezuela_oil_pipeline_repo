# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Silver — Production Clean
# MAGIC **Projeto:** Venezuela Oil Geopolitics
# MAGIC
# MAGIC **Fonte:** `workspace.bronze_venezuela.eia_production`
# MAGIC
# MAGIC **O que essa camada faz:**
# MAGIC - Normaliza unidades para bpd (barrels per day)
# MAGIC - Preenche gaps da série Venezuela com interpolação linear
# MAGIC - Adiciona flag das 3 quebras estruturais do projeto
# MAGIC - Cria date spine mensal 2009→2026 como referência temporal
# MAGIC - Calcula variação mensal (MoM) e anual (YoY)
# MAGIC - Usa MERGE para idempotência — seguro rodar múltiplas vezes
# MAGIC
# MAGIC **Destino:** `workspace.silver_venezuela.production_clean`
# MAGIC
# MAGIC **Quebras estruturais documentadas:**
# MAGIC - `2019-01` — Sanções OFAC na PDVSA
# MAGIC - `2026-01` — Captura de Nicolás Maduro (Op. Absolute Resolve)
# MAGIC - `2026-03` — Fechamento do Estreito de Ormuz

# COMMAND ----------

# MAGIC %md ## 1. Imports e Configuração

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, IntegerType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime
import re

spark = SparkSession.builder.getOrCreate()

# ── Configuração ───────────────────────────────────────────
CATALOG_BRONZE = "workspace"
CATALOG_SILVER = "workspace"
DB_BRONZE      = "bronze_venezuela"
DB_SILVER      = "silver_venezuela"
TBL_SOURCE     = f"{CATALOG_BRONZE}.{DB_BRONZE}.eia_production"
TBL_TARGET     = f"{CATALOG_SILVER}.{DB_SILVER}.production_clean"

# ── Países monitorados ─────────────────────────────────────
COUNTRIES = ["VEN", "BRA", "COL", "IRN", "SAU"]

# ── Quebras estruturais ────────────────────────────────────
# Cada quebra representa um regime diferente na série temporal
STRUCTURAL_BREAKS = [
    {"period": "2019-01", "event": "US_SANCTIONS_PDVSA",
     "description": "Sanções OFAC — embargo total à PDVSA"},
    {"period": "2026-01", "event": "MADURO_CAPTURE",
     "description": "Captura de Maduro — Op. Absolute Resolve"},
    {"period": "2026-03", "event": "HORMUZ_CLOSURE",
     "description": "Fechamento de fato do Estreito de Ormuz"},
]

# Conversão de unidades
TBPD_TO_BPD = 1000   # TBPD (thousand barrels/day) → BPD

print(f"✅ Configuração carregada")
print(f"📥 Fonte  : {TBL_SOURCE}")
print(f"📤 Destino: {TBL_TARGET}")

# COMMAND ----------

# MAGIC %md ## 2. Schema Silver

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_SILVER}.{DB_SILVER}")
print(f"✅ Schema '{CATALOG_SILVER}.{DB_SILVER}' pronto")

# COMMAND ----------

# MAGIC %md ## 3. Leitura e Validação do Bronze

# COMMAND ----------

df_bronze = spark.table(TBL_SOURCE)

print(f"📦 Registros Bronze: {df_bronze.count():,}")
print(f"   Período: {df_bronze.agg(F.min('period')).collect()[0][0]} → "
      f"{df_bronze.agg(F.max('period')).collect()[0][0]}")

# Garante que o Bronze tem dados antes de continuar
assert df_bronze.count() > 0, "❌ Bronze vazio — rode 01_bronze_eia_production primeiro"
assert df_bronze.filter(F.col("country_id") == "VEN").count() > 0, \
    "❌ Sem dados Venezuela no Bronze"

print("✅ Bronze validado — prosseguindo para transformações")

# COMMAND ----------

# MAGIC %md ## 4. Transformações Silver

# COMMAND ----------

# ── 4.1 Cast e limpeza de tipos ────────────────────────────
df_clean = df_bronze \
    .withColumn("value_num",
        F.col("value").cast(DoubleType())) \
    .withColumn("year",
        F.col("period").substr(1, 4).cast(IntegerType())) \
    .withColumn("month",
        F.col("period").substr(6, 2).cast(IntegerType())) \
    .withColumn("period_date",
        F.to_date(F.concat(F.col("period"), F.lit("-01")), "yyyy-MM-dd"))

# ── 4.2 Normalização de unidades → BPD ────────────────────
# Bronze vem em TBPD (thousand barrels per day)
# Silver padroniza tudo em BPD para consistência
df_clean = df_clean \
    .withColumn("production_bpd",
        F.when(F.col("unit") == "TBPD",
               F.col("value_num") * TBPD_TO_BPD)
        .otherwise(F.col("value_num"))) \
    .withColumn("production_tbpd",
        F.when(F.col("unit") == "TBPD", F.col("value_num"))
        .otherwise(F.col("value_num") / TBPD_TO_BPD))

# ── 4.3 Flag de quebras estruturais ───────────────────────
# Cada registro é classificado no regime histórico correto
df_clean = df_clean \
    .withColumn("structural_break_flag",
        F.when(F.col("period") < "2019-01", "PRE_SANCTIONS")
        .when((F.col("period") >= "2019-01") & (F.col("period") < "2026-01"), "POST_SANCTIONS")
        .when((F.col("period") >= "2026-01") & (F.col("period") < "2026-03"), "POST_MADURO")
        .otherwise("POST_HORMUZ")) \
    .withColumn("is_structural_break",
        F.col("period").isin([b["period"] for b in STRUCTURAL_BREAKS]))

# ── 4.4 Variação mensal (MoM) por país ────────────────────
window_country = Window \
    .partitionBy("country_id") \
    .orderBy("period")

df_clean = df_clean \
    .withColumn("production_bpd_lag1",
        F.lag("production_bpd", 1).over(window_country)) \
    .withColumn("mom_change_bpd",
        F.col("production_bpd") - F.col("production_bpd_lag1")) \
    .withColumn("mom_change_pct",
        F.when(F.col("production_bpd_lag1").isNotNull() &
               (F.col("production_bpd_lag1") > 0),
               (F.col("mom_change_bpd") / F.col("production_bpd_lag1")) * 100)
        .otherwise(None))

# ── 4.5 Variação anual (YoY) por país ─────────────────────
df_clean = df_clean \
    .withColumn("production_bpd_lag12",
        F.lag("production_bpd", 12).over(window_country)) \
    .withColumn("yoy_change_pct",
        F.when(F.col("production_bpd_lag12").isNotNull() &
               (F.col("production_bpd_lag12") > 0),
               ((F.col("production_bpd") - F.col("production_bpd_lag12")) /
                F.col("production_bpd_lag12")) * 100)
        .otherwise(None))

# ── 4.6 Interpolação linear de gaps Venezuela ──────────────
# Venezuela tem meses sem reporte — interpolamos linearmente
# apenas para VEN, marcando registros interpolados com flag
window_interp = Window.partitionBy("country_id").orderBy("period")

df_clean = df_clean \
    .withColumn("is_interpolated", F.lit(False)) \
    .withColumn("production_bpd_filled",
        F.when(F.col("production_bpd").isNotNull(), F.col("production_bpd"))
        .otherwise(
            (F.last("production_bpd", ignorenulls=True).over(
                window_interp.rowsBetween(Window.unboundedPreceding, -1)) +
             F.first("production_bpd", ignorenulls=True).over(
                window_interp.rowsBetween(1, Window.unboundedFollowing))) / 2
        )) \
    .withColumn("is_interpolated",
        F.col("production_bpd").isNull() & F.col("production_bpd_filled").isNotNull())

# ── 4.7 Metadados Silver ───────────────────────────────────
df_silver = df_clean \
    .withColumn("silver_timestamp", F.lit(datetime.utcnow().isoformat())) \
    .select(
        # ── Chaves ────────────────────────────────────────
        F.col("period"),
        F.col("period_date"),
        F.col("year"),
        F.col("month"),
        F.col("country_id"),
        F.col("country_name"),
        # ── Produção normalizada ───────────────────────────
        F.col("production_bpd"),
        F.col("production_tbpd"),
        F.col("production_bpd_filled"),
        F.col("is_interpolated"),
        # ── Variações ─────────────────────────────────────
        F.col("mom_change_bpd"),
        F.col("mom_change_pct"),
        F.col("yoy_change_pct"),
        # ── Contexto geopolítico ───────────────────────────
        F.col("structural_break_flag"),
        F.col("is_structural_break"),
        # ── Rastreabilidade ────────────────────────────────
        F.col("ingestion_timestamp").alias("bronze_timestamp"),
        F.col("silver_timestamp"),
        F.col("source"),
    )

print(f"✅ Transformações aplicadas")
print(f"   Registros Silver: {df_silver.count():,}")
print(f"   Colunas: {len(df_silver.columns)}")

# COMMAND ----------

# MAGIC %md ## 5. Salva com MERGE (idempotente)

# COMMAND ----------

# Registra view temporária
df_silver.createOrReplaceTempView("df_silver_tmp")

# Verifica se tabela Delta existe
try:
    dt = DeltaTable.forName(spark, TBL_TARGET)
    is_delta = True
except Exception as e:
    is_delta = False

# Verifica se tabela existe (mesmo não sendo Delta)
try:
    count_existing = spark.table(TBL_TARGET).count()
    table_exists = True
except:
    table_exists = False
    count_existing = 0

print(f"   Tabela existe: {table_exists}")
print(f"   É Delta: {is_delta}")
print(f"   Registros existentes: {count_existing:,}")

if is_delta and table_exists:
    # MERGE seguro
    dt.alias("target") \
      .merge(
          df_silver.alias("source"),
          "target.period = source.period AND target.country_id = source.country_id"
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
    print("✅ MERGE executado")

else:
    # Drop e recria como Delta
    spark.sql(f"DROP TABLE IF EXISTS {TBL_TARGET}")

    df_silver.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "country_id") \
        .saveAsTable(TBL_TARGET)
    print("✅ Tabela Delta criada")

total = spark.table(TBL_TARGET).count()
print(f"   Total: {total:,}")

# COMMAND ----------

# MAGIC %md ## 6. Validação Silver — PySpark Nativo

# COMMAND ----------

df_val  = spark.table(TBL_TARGET)
total   = df_val.count()
results = []

def check(description, passed, detail=""):
    status = "✅" if passed else "❌"
    msg = f"   {status} {description}"
    if detail: msg += f" — {detail}"
    print(msg)
    results.append((description, passed))

print("🔍 Validações Silver — Production Clean")
print()

# ── 1. Volume ──────────────────────────────────────────────
check("Volume total >= 300", total >= 300, f"{total:,}")

# ── 2. Colunas obrigatórias sem nulos ──────────────────────
for c in ["period", "country_id", "structural_break_flag",
          "silver_timestamp", "source"]:
    nulls = df_val.filter(F.col(c).isNull()).count()
    check(f"'{c}' sem nulos", nulls == 0, f"{nulls} nulos")

# ── 3. Unidades normalizadas ───────────────────────────────
df_com_prod = df_val.filter(F.col("production_bpd").isNotNull())
prod_min = df_com_prod.agg({"production_bpd": "min"}).collect()[0][0]
prod_max = df_com_prod.agg({"production_bpd": "max"}).collect()[0][0]
check("production_bpd >= 0", prod_min >= 0, f"mínimo: {prod_min:,.0f}")
check("production_bpd <= 20.000.000", prod_max <= 20000000,
      f"máximo: {prod_max:,.0f}")

# ── 4. Regimes — flexível ao período disponível ───────────
periodo_max = df_val.agg({"period": "max"}).collect()[0][0]
regimes = set(r["structural_break_flag"] for r in
              df_val.select("structural_break_flag").distinct().collect())

regimes_esperados = {"PRE_SANCTIONS", "POST_SANCTIONS"}
if periodo_max >= "2026-01":
    regimes_esperados.add("POST_MADURO")
if periodo_max >= "2026-03":
    regimes_esperados.add("POST_HORMUZ")

check(f"Regimes esperados para período até {periodo_max}",
      regimes_esperados.issubset(regimes), str(sorted(regimes)))

# ── 5. Sem duplicatas ──────────────────────────────────────
# Conta distintos por chave natural
total_distinct = df_val.select("period", "country_id").distinct().count()
# Aceita múltiplos se MERGE ainda não foi aplicado — avisa mas não falha
if total != total_distinct:
    multiplos = total // total_distinct
    print(f"   ⚠️  Dados duplicados {multiplos}x — drop e rerun necessário")
    print(f"      Execute: spark.sql('DROP TABLE IF EXISTS {TBL_TARGET}')")
    results.append(("Sem duplicatas", False))
else:
    check("Sem duplicatas period+country_id",
          True, f"{total:,} = {total_distinct:,} distintos")

# ── 6. Variações calculadas ────────────────────────────────
com_mom = df_val.filter(F.col("mom_change_pct").isNotNull()).count()
com_yoy = df_val.filter(F.col("yoy_change_pct").isNotNull()).count()
check("MoM calculado para >= 80%", com_mom / total >= 0.80, f"{com_mom:,}")
check("YoY calculado para >= 60%", com_yoy / total >= 0.60, f"{com_yoy:,}")

# ── 7. Venezuela por regime ────────────────────────────────
print()
print("   ── Venezuela por regime geopolítico ───────────────")
df_ven = df_val.filter(F.col("country_id") == "VEN")
df_ven.groupBy("structural_break_flag") \
    .agg(F.avg("production_bpd_filled").alias("media_bpd"),
         F.count("period").alias("meses")) \
    .orderBy("structural_break_flag") \
    .show()

interp_count = df_ven.filter(F.col("is_interpolated")).count()
total_ven    = df_ven.count()
print(f"   📊 Gaps interpolados VEN: {interp_count} de {total_ven} meses "
      f"({100*interp_count/total_ven:.1f}%)")

# ── Resultado ──────────────────────────────────────────────
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
    if any(desc == "Sem duplicatas" and not ok for desc, ok in results):
        print()
        print("💡 Para corrigir duplicatas:")
        print(f"   spark.sql('DROP TABLE IF EXISTS {TBL_TARGET}')")
        print("   Depois rode o notebook do início com Run all")
    raise ValueError(f"Silver validation failed: production_clean")
else:
    print("✅ Silver production_clean — aprovado para Gold")
print("=" * 55)

# COMMAND ----------

total = spark.table("workspace.silver_venezuela.production_clean").count()
distinct = spark.table("workspace.silver_venezuela.production_clean") \
    .select("period", "country_id").distinct().count()

print(f"Total: {total:,}")
print(f"Distintos: {distinct:,}")
print(f"Ratio: {total/distinct:.1f}x")

# COMMAND ----------

# Verifica duplicatas no Bronze
df_bronze = spark.table("workspace.bronze_venezuela.eia_production")

total_bronze = df_bronze.count()
distinct_bronze = df_bronze.select("period", "country_id").distinct().count()

print(f"Bronze — Total: {total_bronze:,}")
print(f"Bronze — Distintos: {distinct_bronze:,}")
print(f"Bronze — Ratio: {total_bronze/distinct_bronze:.1f}x")
