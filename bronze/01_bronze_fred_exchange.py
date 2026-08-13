# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01_bronze_fred_exchange
# MAGIC
# MAGIC Ingestão Bronze da série **DEXVZUS** (Bolívar/Dólar, Federal Reserve Board, release H.10).
# MAGIC
# MAGIC Fonte: FRED — endpoint público, sem chave de API.
# MAGIC Chave de merge: `data_referencia` (Documento de Padrões v2.0, seção 4.1).
# MAGIC Gate aplicável ao final deste notebook: seção 6.1 do Documento de Padrões v2.0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração
# MAGIC
# MAGIC `SERIES_ID` isolado como variável — nunca hardcoded inline. Lição herdada da
# MAGIC iteração anterior do projeto, quando a EIA renomeou `BREPUUS`/`WTIPUUS` para
# MAGIC `RBRTE`/`RWTC` sem aviso e quebrou notebooks que tinham o ID cravado no meio do código.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
import pandas as pd
import requests
import io

CATALOG = "workspace"
SCHEMA = "bronze_venezuela"
TABLE = "fred_exchange"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE}"

SERIES_ID = "DEXVZUS"
BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
SOURCE_URL = f"{BASE_URL}?id={SERIES_ID}"

MERGE_KEY = "data_referencia"
SOURCE_SYSTEM = "fred"
TRUST_TIER = "primario_oficial"
MIN_COVERAGE_DATE = "2025-01-01"  # cobertura mínima exigida pelo Gate 6.1, item 4

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract
# MAGIC
# MAGIC Download direto do CSV público. Sem autenticação, sem SDK — apenas HTTP.

# COMMAND ----------

response = requests.get(SOURCE_URL, timeout=30)
response.raise_for_status()
raw_csv = response.text

print(f"Status: {response.status_code} | bytes recebidos: {len(response.content)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform
# MAGIC
# MAGIC Parse por **posição de coluna**, não por nome — o cabeçalho do FRED já mudou
# MAGIC entre releases no passado, então confiar no nome da coluna é frágil.
# MAGIC
# MAGIC Dias sem publicação vêm marcados com `"."` (ND). `pd.to_numeric(..., errors="coerce")`
# MAGIC converte esse marcador para `NaN` automaticamente; as linhas resultantes são
# MAGIC descartadas — isso é o **gap** documentado na ficha da fonte, e é intencional:
# MAGIC o FRED não repete o último valor, ele simplesmente não publica. Diferente do
# MAGIC comportamento esperado da BCV Today no próximo notebook.

# COMMAND ----------

pdf_raw = pd.read_csv(io.StringIO(raw_csv))
pdf = pdf_raw.iloc[:, [0, 1]].copy()
pdf.columns = ["data_referencia", "valor_cambio"]

pdf["valor_cambio"] = pd.to_numeric(pdf["valor_cambio"], errors="coerce")
pdf["data_referencia"] = pd.to_datetime(pdf["data_referencia"]).dt.date

linhas_brutas = len(pdf)
pdf = pdf.dropna(subset=["valor_cambio"]).reset_index(drop=True)
linhas_gap = linhas_brutas - len(pdf)

print(f"Linhas brutas: {linhas_brutas} | Gaps descartados: {linhas_gap} | Linhas válidas: {len(pdf)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enriquecimento — 5 colunas de metadados obrigatórias (Documento v2.0, seção 4.2)
# MAGIC
# MAGIC `is_carried_forward` é sempre `False` para o FRED: por definição da fonte,
# MAGIC não existe repetição de valor, só gap.

# COMMAND ----------

df_new = (
    spark.createDataFrame(pdf[["data_referencia", "valor_cambio"]])
    .withColumn("data_referencia", F.col("data_referencia").cast("date"))
    .withColumn("valor_cambio", F.col("valor_cambio").cast("double"))
    .withColumn("source_system", F.lit(SOURCE_SYSTEM))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_url", F.lit(SOURCE_URL))
    .withColumn("trust_tier", F.lit(TRUST_TIER))
    .withColumn("is_carried_forward", F.lit(False))
    .select(
        "data_referencia", "valor_cambio", "source_system",
        "ingestion_timestamp", "source_url", "trust_tier", "is_carried_forward",
    )
)

df_new.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load — criação da tabela Bronze (idempotente na criação) e MERGE
# MAGIC
# MAGIC Nunca `mode("append")` isolado — essa foi a causa raiz do descarte da primeira
# MAGIC iteração do projeto (~5x duplicação). Todo upsert aqui passa por `MERGE`.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {FULL_TABLE_NAME} (
    data_referencia     DATE      NOT NULL COMMENT 'Chave de merge — data da cotação',
    valor_cambio        DOUBLE    NOT NULL COMMENT 'VES por USD, noon buying rate NY',
    source_system       STRING    NOT NULL COMMENT '"fred" — fixo',
    ingestion_timestamp TIMESTAMP NOT NULL COMMENT 'Momento da carga no Bronze',
    source_url          STRING    NOT NULL COMMENT 'Endpoint exato consultado',
    trust_tier          STRING    NOT NULL COMMENT '"primario_oficial"',
    is_carried_forward  BOOLEAN   NOT NULL COMMENT 'Sempre False para o FRED — fonte gera gap, não repetição'
)
USING DELTA
COMMENT 'Bronze — câmbio VES/USD, série DEXVZUS (Federal Reserve Board, release H.10). Chave de merge: data_referencia.'
""")

bronze_table = DeltaTable.forName(spark, FULL_TABLE_NAME)

# COMMAND ----------


def merge_upsert():
    (
        bronze_table.alias("target")
        .merge(df_new.alias("source"), f"target.{MERGE_KEY} = source.{MERGE_KEY}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


merge_upsert()
print(f"MERGE executado em {FULL_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate 6.1 — Bronze → Silver
# MAGIC
# MAGIC Sete checagens mecânicas (Documento de Padrões v2.0, seção 6.1). A checagem 1
# MAGIC roda o MERGE uma segunda vez, na própria célula, e compara a contagem de linhas
# MAGIC antes/depois — é a prova de idempotência exigida pelo critério de aceite da seção 4.1.

# COMMAND ----------

print("=" * 62)
print("GATE 6.1 — Bronze → Silver | 01_bronze_fred_exchange")
print("=" * 62)

gate_results = {}

# ---- 1. Idempotência (prova auto-executável) ----
count_antes = spark.table(FULL_TABLE_NAME).count()
merge_upsert()
count_depois = spark.table(FULL_TABLE_NAME).count()

gate_results["1. Idempotencia"] = count_antes == count_depois
print(
    f"1. Idempotência ............. {'PASS' if gate_results['1. Idempotencia'] else 'FAIL'} "
    f"(antes={count_antes}, depois={count_depois})"
)

df_check = spark.table(FULL_TABLE_NAME)

# ---- 2. Completude de metadados ----
metadata_cols = ["source_system", "ingestion_timestamp", "trust_tier"]
nulls_metadata = (
    df_check.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in metadata_cols])
    .collect()[0]
    .asDict()
)
gate_results["2. Metadados"] = all(v == 0 for v in nulls_metadata.values())
print(f"2. Completude de metadados ... {'PASS' if gate_results['2. Metadados'] else 'FAIL'} {nulls_metadata}")

# ---- 3. Sem duplicatas na chave de merge ----
total = df_check.count()
distintos = df_check.select(MERGE_KEY).distinct().count()
gate_results["3. Sem duplicatas"] = total == distintos
print(f"3. Sem duplicatas na chave ... {'PASS' if gate_results['3. Sem duplicatas'] else 'FAIL'} (total={total}, distintos={distintos})")

# ---- 4. Cobertura mínima de data ----
min_data = df_check.agg(F.min("data_referencia")).collect()[0][0]
gate_results["4. Cobertura minima"] = str(min_data) <= MIN_COVERAGE_DATE
print(f"4. Cobertura mínima .......... {'PASS' if gate_results['4. Cobertura minima'] else 'FAIL'} (min={min_data})")

# ---- 5. Sem nulos em colunas críticas ----
nulos_criticos = df_check.filter(
    F.col("data_referencia").isNull() | F.col("valor_cambio").isNull()
).count()
gate_results["5. Sem nulos criticos"] = nulos_criticos == 0
print(f"5. Sem nulos críticos ........ {'PASS' if gate_results['5. Sem nulos criticos'] else 'FAIL'} (nulos={nulos_criticos})")

# ---- 6. Faixa de valores plausível ----
stats = df_check.agg(F.min("valor_cambio").alias("min"), F.max("valor_cambio").alias("max")).collect()[0]
faixa_ok = stats["min"] > 0 and (stats["max"] / stats["min"] < 100)
gate_results["6. Faixa plausivel"] = faixa_ok
print(f"6. Faixa de valores .......... {'PASS' if gate_results['6. Faixa plausivel'] else 'FAIL'} (min={stats['min']}, max={stats['max']})")

# ---- 7. is_carried_forward correto para o FRED (sempre False) ----
carried_forward_incorreto = df_check.filter(F.col("is_carried_forward") == True).count()  # noqa: E712
gate_results["7. is_carried_forward"] = carried_forward_incorreto == 0
print(f"7. is_carried_forward ........ {'PASS' if gate_results['7. is_carried_forward'] else 'FAIL'} (linhas incorretas={carried_forward_incorreto})")

print("=" * 62)
todos_passaram = all(gate_results.values())
print(f"RESULTADO GERAL: {'PASS — pronto para promoção' if todos_passaram else 'FAIL — corrigir antes de prosseguir'}")
print("=" * 62)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da execução

# COMMAND ----------

max_data = df_check.agg(F.max("data_referencia")).collect()[0][0]

print(f"""
RESUMO — 01_bronze_fred_exchange
---------------------------------
Tabela               {FULL_TABLE_NAME}
Série                {SERIES_ID}
Linhas brutas (CSV)  {linhas_brutas}
Gaps descartados     {linhas_gap}
Linhas no Bronze      {count_depois}
Cobertura            {min_data} até {max_data}
Gate 6.1              {'PASS' if todos_passaram else 'FAIL'}
""")
