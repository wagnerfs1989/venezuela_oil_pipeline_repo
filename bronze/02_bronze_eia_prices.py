# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — EIA Prices
# MAGIC **Projeto:** Venezuela Oil Geopolitics
# MAGIC
# MAGIC **Fonte:** EIA API v2 — Petroleum Spot Prices
# MAGIC
# MAGIC **Dado:** Preços diários Brent e WTI em USD/barril
# MAGIC
# MAGIC **Destino:** `workspace.bronze_venezuela.eia_prices` (Delta Lake gerenciado)
# MAGIC
# MAGIC **Validação:** PySpark nativo
# MAGIC
# MAGIC ---
# MAGIC > ⚠️ **Antes de rodar:** substitua `EIA_API_KEY` na Célula 1

# COMMAND ----------

# MAGIC %md ## 1. Imports e Configuração

# COMMAND ----------

import requests
import json
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# ── Secrets ────────────────────────────────────────────────
EIA_API_KEY = "SUA_CHAVE_AQUI"   # ← substitua pela sua chave real

# ── Configuração Unity Catalog ─────────────────────────────
CATALOG    = "workspace"
DB_BRONZE  = "bronze_venezuela"
TABLE_NAME = "eia_prices"
FULL_TABLE = f"{CATALOG}.{DB_BRONZE}.{TABLE_NAME}"

PRICE_SERIES = {
    "RBRTE": "Brent",
    "RWTC":  "WTI",
}

print(f"✅ Configuração carregada")
print(f"🗄️  Tabela destino: {FULL_TABLE}")

# COMMAND ----------

# MAGIC %md ## 2. Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DB_BRONZE}")
print(f"✅ Schema '{CATALOG}.{DB_BRONZE}' pronto")

# COMMAND ----------

# MAGIC %md ## 3. Funções

# COMMAND ----------

def fetch_eia(endpoint, params):
    base_url = "https://api.eia.gov/v2"
    params["api_key"] = EIA_API_KEY
    try:
        r = requests.get(f"{base_url}/{endpoint}", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if "error" in data:
            raise ValueError(f"EIA API error: {data['error']}")
        return data
    except requests.exceptions.Timeout:
        raise RuntimeError("⏱️ EIA API timeout")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"❌ HTTP {e.response.status_code}: {e.response.text[:300]}")


def normalize_prices(records, series_id, series_label):
    ingestion_ts = datetime.utcnow().isoformat()
    normalized   = []
    for r in records:
        clean = {
            "period":      r.get("period"),
            "series_id":   series_id,
            "series_name": series_label,
            "value":       r.get("value"),
            "value_str":   str(r.get("value", "")),
            "unit":        "USD/bbl",
            "ingestion_timestamp": ingestion_ts,
            "source":      "EIA_API_v2",
        }
        clean["raw_json"] = json.dumps({k: v for k, v in clean.items()
                                        if k not in ["ingestion_timestamp","source","raw_json"]})
        normalized.append(clean)
    return normalized


def save_managed_delta(records, full_table):
    if not records:
        print("⚠️  Sem dados.")
        return
    df = spark.createDataFrame(records)
    df.write.format("delta").mode("append").saveAsTable(full_table)
    total = spark.table(full_table).count()
    print(f"✅ {len(records):,} inseridos | Total: {total:,}")


print("✅ Funções carregadas")

# COMMAND ----------

# MAGIC %md ## 4. Ingestão — Brent e WTI

# COMMAND ----------

all_records = []

for series_id, label in PRICE_SERIES.items():
    print(f"🔄 Buscando {label} ({series_id})...")
    params = {
        "frequency":          "daily",
        "data[0]":            "value",
        "facets[series][]":   series_id,
        "sort[0][column]":    "period",
        "sort[0][direction]": "desc",
        "offset":             0,
        "length":             3000,
    }
    raw  = fetch_eia("petroleum/pri/spt/data/", params)
    recs = raw.get("response", {}).get("data", [])
    print(f"   → {len(recs)} registros | {recs[-1]['period']} → {recs[0]['period']}")
    all_records.extend(normalize_prices(recs, series_id, label))

print(f"\n📦 Total normalizado: {len(all_records):,}")

# COMMAND ----------

# MAGIC %md ## 5. Salva no Delta Lake

# COMMAND ----------

save_managed_delta(all_records, FULL_TABLE)

# COMMAND ----------

# MAGIC %md ## 6. Validação Bronze — PySpark Nativo

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

print("🔍 Validações Bronze — EIA Prices")
print()

# ── 1. Volume total ────────────────────────────────────────
check("Volume total >= 1000 registros", total >= 1000, f"{total:,}")

# ── 2. Colunas obrigatórias não nulas ──────────────────────
for c in ["period", "series_id", "ingestion_timestamp", "raw_json", "source"]:
    nulls = df_val.filter(col(c).isNull()).count()
    check(f"'{c}' sem nulos", nulls == 0, f"{nulls} nulos")

# ── 3. Séries esperadas ────────────────────────────────────
series_encontradas = [r["series_id"] for r in df_val.select("series_id").distinct().collect()]
series_ok = set(series_encontradas) == set(PRICE_SERIES.keys())
check("Brent e WTI presentes", series_ok, str(series_encontradas))

# ── 4. Formato do período YYYY-MM-DD ──────────────────────
sample = [r["period"] for r in df_val.select("period").limit(200).collect()]
fmt_ok = all(re.match(r"^\d{4}-\d{2}-\d{2}$", p) for p in sample if p)
check("period no formato YYYY-MM-DD", fmt_ok)

# ── 5. Período não no futuro ───────────────────────────────
periodo_max = df_val.agg({"period": "max"}).collect()[0][0]
check("period não no futuro (≤ 2027-12-31)", periodo_max <= "2027-12-31", f"máximo: {periodo_max}")

# ── 6. Volume por série ────────────────────────────────────
print()
print("   ── Volume por série ────────────────────────────────")
for sid, label in PRICE_SERIES.items():
    count = df_val.filter(col("series_id") == sid).count()
    check(f"  {label}: >= 500 registros", count >= 500, f"{count:,}")

# ── 7. Range de preço — mínimo ajustado para aceitar negativos ────
df_com_valor = df_val.filter(col("value").isNotNull())
from pyspark.sql.functions import col as spark_col
from pyspark.sql.types import DoubleType

df_com_valor = df_com_valor.withColumn("value_num", 
    spark_col("value").cast(DoubleType())) \
    .filter(spark_col("value_num").isNotNull())

price_min = df_com_valor.agg({"value_num": "min"}).collect()[0][0]
price_max = df_com_valor.agg({"value_num": "max"}).collect()[0][0]
print()
check("value >= -$50/bbl (aceita negativos históricos)", 
    price_min >= -50.0, f"mínimo: ${price_min:.2f}")
check("value <= $250/bbl", 
    price_max <= 250.0, f"máximo: ${price_max:.2f}")

# ── 8. Nulos em value (fins de semana esperados) ───────────
nulls_value = df_val.filter(col("value").isNull()).count()
null_pct    = nulls_value / total
check("nulos em 'value' < 35%", null_pct < 0.35, f"{null_pct:.1%}")

# ── 9. Source correto ──────────────────────────────────────
sources = [r["source"] for r in df_val.select("source").distinct().collect()]
check("source = 'EIA_API_v2'", sources == ["EIA_API_v2"], str(sources))

# ── Extremos Brent com cast numérico ──────────────────────
brent = df_val.filter((col("series_id") == "RBRTE") & col("value").isNotNull()) \
    .withColumn("value_num", col("value").cast(DoubleType()))

brent_max = brent.orderBy(col("value_num").desc()).select("period","value_num").first()
brent_min = brent.orderBy(col("value_num").asc()).select("period","value_num").first()
print()
print(f"   📈 Brent máximo: USD {brent_max['value_num']:.2f}/bbl em {brent_max['period']}")
print(f"   📉 Brent mínimo: USD {brent_min['value_num']:.2f}/bbl em {brent_min['period']}")

# ── Resultado final ────────────────────────────────────────
print()
passed = sum(1 for _, ok in results if ok)
failed = len(results) - passed

print("=" * 55)
print(f"📊 RESULTADO: {passed}/{len(results)} validações passaram")

if failed > 0:
    print(f"\n❌ {failed} falha(s) — corrigir antes de avançar para Silver:")
    for desc, ok in results:
        if not ok:
            print(f"   → {desc}")
    raise ValueError(f"Bronze validation failed: {TABLE_NAME}")
else:
    print(f"✅ Bronze {TABLE_NAME} — aprovado para Silver")
print("=" * 55)
