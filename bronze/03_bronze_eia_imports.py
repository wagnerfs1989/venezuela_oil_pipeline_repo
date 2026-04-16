# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Bronze — EIA Imports USA ← Países Produtores
# MAGIC **Projeto:** Venezuela Oil Geopolitics
# MAGIC
# MAGIC **Fonte:** EIA API v2 — Crude Oil Imports (EIA-814)
# MAGIC
# MAGIC **Dado:** Volume mensal de petróleo importado pelos EUA por país de origem
# MAGIC
# MAGIC **Cobertura:** 2009 → hoje (histórico completo via paginação)
# MAGIC
# MAGIC **Destino:** `workspace.bronze_venezuela.eia_imports` (Delta Lake gerenciado)
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
TABLE_NAME = "eia_imports"
FULL_TABLE = f"{CATALOG}.{DB_BRONZE}.{TABLE_NAME}"

IMPORT_ORIGINS = {
    "CTY_VE": "Venezuela",
    "CTY_SA": "Saudi Arabia",
    "CTY_CA": "Canada",
    "CTY_IZ": "Iraq",
    "CTY_BR": "Brazil",
    "CTY_CO": "Colombia",
}

PAGE_SIZE = 5000

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

def fetch_all_pages(origin_id, origin_name):
    all_records = []
    offset      = 0
    print(f"   🔄 Paginando {origin_name}...")
    while True:
        params = {
            "api_key":            EIA_API_KEY,
            "frequency":          "monthly",
            "data[0]":            "quantity",
            "facets[originId][]": origin_id,
            "sort[0][column]":    "period",
            "sort[0][direction]": "asc",
            "offset":             offset,
            "length":             PAGE_SIZE,
        }
        try:
            r = requests.get("https://api.eia.gov/v2/crude-oil-imports/data/",
                             params=params, timeout=30)
            r.raise_for_status()
            data    = r.json()
            records = data.get("response", {}).get("data", [])
            total   = int(data.get("response", {}).get("total", 0))
        except Exception as e:
            print(f"   ❌ Erro offset={offset}: {e}")
            break
        if not records:
            break
        all_records.extend(records)
        offset += len(records)
        print(f"      → {offset:,} / {total:,}")
        if offset >= total:
            break
    return all_records


def normalize_imports(records, origin_id, origin_name):
    ingestion_ts = datetime.utcnow().isoformat()
    normalized   = []
    for r in records:
        clean = {
            "period":           r.get("period"),
            "origin_id":        origin_id,
            "origin_name":      origin_name,
            "destination_id":   r.get("destinationId", ""),
            "destination_name": r.get("destinationName", ""),
            "grade_id":         r.get("gradeId", ""),
            "grade_name":       r.get("gradeName", ""),
            "quantity":         r.get("quantity"),
            "quantity_str":     str(r.get("quantity", "")),
            "unit":             "thousand barrels",
            "ingestion_timestamp": ingestion_ts,
            "source":           "EIA_API_v2_crude_imports",
        }
        clean["raw_json"] = json.dumps({k: v for k, v in clean.items()
                                        if k not in ["ingestion_timestamp","source","raw_json"]})
        normalized.append(clean)
    return normalized


def save_managed_delta(records, full_table):
    if not records:
        print("   ⚠️  Sem dados.")
        return
    df = spark.createDataFrame(records)
    df.write.format("delta").mode("append").saveAsTable(full_table)
    total = spark.table(full_table).count()
    print(f"   ✅ {len(records):,} inseridos | Total: {total:,}")


print("✅ Funções carregadas")

# COMMAND ----------

# MAGIC %md ## 4. Ingestão com Paginação

# COMMAND ----------

summary = []

for origin_id, origin_name in IMPORT_ORIGINS.items():
    print(f"\n{'='*50}")
    print(f"🌍 {origin_name} ({origin_id})")
    raw_records = fetch_all_pages(origin_id, origin_name)
    if not raw_records:
        summary.append((origin_name, 0, "-", "-"))
        continue
    periodo_inicio = raw_records[0].get("period", "?")
    periodo_fim    = raw_records[-1].get("period", "?")
    print(f"   📅 {periodo_inicio} → {periodo_fim}")
    normalized = normalize_imports(raw_records, origin_id, origin_name)
    save_managed_delta(normalized, FULL_TABLE)
    summary.append((origin_name, len(normalized), periodo_inicio, periodo_fim))

print("\n" + "=" * 65)
print("📋 RESUMO DA INGESTÃO")
print("=" * 65)
for nome, qtd, inicio, fim in summary:
    print(f"   {nome:<20} {qtd:>10,} | {inicio} → {fim}")
print(f"   {'TOTAL':<20} {sum(q for _,q,_,_ in summary):>10,}")
print("=" * 65)

# COMMAND ----------

# MAGIC %md ## 5. Salva no Delta Lake
# MAGIC > Dados já salvos durante a ingestão acima.

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

print("🔍 Validações Bronze — EIA Imports")
print()

# ── 1. Volume total ────────────────────────────────────────
check("Volume total >= 10.000 registros", total >= 10000, f"{total:,}")

# ── 2. Colunas obrigatórias não nulas ──────────────────────
for c in ["period", "origin_id", "ingestion_timestamp", "raw_json", "source"]:
    nulls = df_val.filter(col(c).isNull()).count()
    check(f"'{c}' sem nulos", nulls == 0, f"{nulls} nulos")

# ── 3. Países esperados ────────────────────────────────────
paises = [r["origin_id"] for r in df_val.select("origin_id").distinct().collect()]
paises_ok = set(paises) == set(IMPORT_ORIGINS.keys())
check("Todos os países monitorados presentes", paises_ok, str(sorted(paises)))

# ── 4. Formato do período YYYY-MM ──────────────────────────
sample = [r["period"] for r in df_val.select("period").limit(200).collect()]
fmt_ok = all(re.match(r"^\d{4}-\d{2}$", p) for p in sample if p)
check("period no formato YYYY-MM", fmt_ok)

# ── 5. Período não no futuro ───────────────────────────────
periodo_max = df_val.agg({"period": "max"}).collect()[0][0]
check("period não no futuro (≤ 2027-12)", periodo_max <= "2027-12", f"máximo: {periodo_max}")

# ── 6. Cobertura histórica Venezuela ──────────────────────
ven_min = df_val.filter(col("origin_id") == "CTY_VE") \
    .agg({"period": "min"}).collect()[0][0]
check("Histórico VEN começa ≤ 2010-01", ven_min <= "2010-01", f"início: {ven_min}")

# ── 7. Volume Venezuela ────────────────────────────────────
ven_count = df_val.filter(col("origin_id") == "CTY_VE").count()
check("Volume Venezuela >= 1.000 registros", ven_count >= 1000, f"{ven_count:,}")

# ── 8. Quantity não negativa ───────────────────────────────
from pyspark.sql.types import DoubleType

df_qty  = df_val.filter(col("quantity").isNotNull()) \
    .withColumn("quantity_num", col("quantity").cast(DoubleType())) \
    .filter(col("quantity_num").isNotNull())

qty_min = df_qty.agg({"quantity_num": "min"}).collect()[0][0]
check("quantity >= 0", qty_min >= 0, f"mínimo: {qty_min:,.0f}")

# ── 9. Insight sanções Venezuela ───────────────────────────
df_ven = df_val.filter((col("origin_id") == "CTY_VE") & col("quantity").isNotNull()) \
    .withColumn("quantity_num", col("quantity").cast(DoubleType()))

pre = df_ven.filter(col("period") < "2019-01").agg({"quantity_num": "avg"}).collect()[0][0]
pos = df_ven.filter(col("period") >= "2019-01").agg({"quantity_num": "avg"}).collect()[0][0]
if pre and pos:
    reducao = (1 - pos / pre) * 100
    print()
    print(f"   📊 Impacto das sanções Venezuela:")
    print(f"      Pré-2019 : {pre:,.0f} thousand barrels/mês")
    print(f"      Pós-2019 : {pos:,.0f} thousand barrels/mês")
    print(f"      Redução  : {reducao:.1f}%")

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
