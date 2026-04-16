# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Bronze — EIA Production
# MAGIC **Projeto:** Venezuela Oil Geopolitics
# MAGIC
# MAGIC **Fonte:** U.S. Energy Information Administration (EIA) API v2
# MAGIC
# MAGIC **Dado:** Produção mensal de petróleo bruto — Venezuela + países contexto
# MAGIC
# MAGIC **Destino:** `workspace.bronze_venezuela.eia_production` (Delta Lake gerenciado)
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
TABLE_NAME = "eia_production"
FULL_TABLE = f"{CATALOG}.{DB_BRONZE}.{TABLE_NAME}"
COUNTRIES  = ["VEN", "BRA", "COL", "IRN", "SAU"]

print(f"✅ Configuração carregada")
print(f"🗄️  Tabela destino: {FULL_TABLE}")

# COMMAND ----------

# MAGIC %md ## 2. Schema

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{DB_BRONZE}")
print(f"✅ Schema '{CATALOG}.{DB_BRONZE}' pronto")

# COMMAND ----------

# MAGIC %md ## 3. Funções de Ingestão

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


def normalize_production(records):
    ingestion_ts = datetime.utcnow().isoformat()
    normalized   = []
    for r in records:
        clean = {
            "period":              r.get("period"),
            "country_id":         r.get("countryRegionId"),
            "country_name":       r.get("countryRegionName"),
            "product_id":         str(r.get("productId", "")),
            "product_name":       r.get("productName", ""),
            "unit":               r.get("unit", ""),
            "value":              r.get("value"),
            "value_str":          str(r.get("value", "")),
            "ingestion_timestamp": ingestion_ts,
            "source":             "EIA_API_v2",
        }
        clean["raw_json"] = json.dumps({k: v for k, v in clean.items()
                                        if k not in ["ingestion_timestamp","source","raw_json"]})
        normalized.append(clean)
    return normalized


def save_managed_delta(records, full_table):
    if not records:
        print("⚠️  Nenhum registro para salvar.")
        return
    df = spark.createDataFrame(records)
    df.write.format("delta").mode("append").saveAsTable(full_table)
    total = spark.table(full_table).count()
    print(f"✅ {len(records):,} inseridos | Total acumulado: {total:,}")


print("✅ Funções carregadas")

# COMMAND ----------

# MAGIC %md ## 4. Ingestão

# COMMAND ----------

all_records = []

for country in COUNTRIES:
    print(f"🔄 Buscando produção: {country}...")
    params = {
        "frequency":                 "monthly",
        "data[0]":                   "value",
        "facets[countryRegionId][]": country,
        "facets[productId][]":       "57",
        "sort[0][column]":           "period",
        "sort[0][direction]":        "desc",
        "offset":                    0,
        "length":                    180,
    }
    raw     = fetch_eia("international/data/", params)
    records = raw.get("response", {}).get("data", [])
    print(f"   → {len(records)} registros | {records[-1]['period']} → {records[0]['period']}")
    all_records.extend(normalize_production(records))

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

print("🔍 Validações Bronze — EIA Production")
print()

# ── 1. Volume total ────────────────────────────────────────
check("Volume total >= 300 registros", total >= 300, f"{total:,}")

# ── 2. Colunas obrigatórias não nulas ──────────────────────
for c in ["period", "country_id", "ingestion_timestamp", "raw_json", "source"]:
    nulls = df_val.filter(col(c).isNull()).count()
    check(f"'{c}' sem nulos", nulls == 0, f"{nulls} nulos encontrados")

# ── 3. Formato do período YYYY-MM ──────────────────────────
sample   = [r["period"] for r in df_val.select("period").limit(200).collect()]
fmt_ok   = all(re.match(r"^\d{4}-\d{2}$", p) for p in sample if p)
check("period no formato YYYY-MM", fmt_ok)

# ── 4. Período não no futuro ───────────────────────────────
periodo_max = df_val.agg({"period": "max"}).collect()[0][0]
check("period não no futuro (≤ 2027-12)", periodo_max <= "2027-12", f"máximo: {periodo_max}")

# ── 5. Período histórico mínimo ────────────────────────────
periodo_min = df_val.agg({"period": "min"}).collect()[0][0]
check("histórico começa ≤ 2015-01", periodo_min <= "2015-01", f"mínimo: {periodo_min}")

# ── 6. Países esperados ────────────────────────────────────
paises_encontrados = [r["country_id"] for r in df_val.select("country_id").distinct().collect()]
paises_ok = all(p in COUNTRIES for p in paises_encontrados)
check("country_id contém apenas países monitorados", paises_ok, str(paises_encontrados))

# ── 7. Volume mínimo por país ──────────────────────────────
print()
print("   ── Volume por país ────────────────────────────────")
for country in COUNTRIES:
    count = df_val.filter(col("country_id") == country).count()
    ok    = count >= 60
    check(f"  {country}: >= 60 registros", ok, f"{count:,}")

# ── 8. Nulos em value (gaps VEN esperados, mas < 30%) ──────
nulls_value = df_val.filter(col("value").isNull()).count()
null_pct    = nulls_value / total
print()
check("nulos em 'value' < 30%", null_pct < 0.30, f"{null_pct:.1%}")

# ── 9. Source correto ──────────────────────────────────────
sources = [r["source"] for r in df_val.select("source").distinct().collect()]
check("source = 'EIA_API_v2'", sources == ["EIA_API_v2"], str(sources))

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
