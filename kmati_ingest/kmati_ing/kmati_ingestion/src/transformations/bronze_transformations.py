# transformations/bronze_transformations.py

from __future__ import annotations
from typing import Dict, Any

from pyspark.sql import DataFrame, functions as F, types as T
import uuid


# ---------- 1. NORMALIZE COLUMN NAMES ----------

def _to_snake_case(name: str) -> str:
    return (
        name.strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .lower()
    )


def normalize_column_names(
    df: DataFrame,
    strategy: str | None
) -> DataFrame:
    """
    strategy:
      - "snake_case" -> loan_id, disbursement_dt
      - "lower"      -> loanid, disbursementdt
      - None / other -> no change
    """
    if not strategy:
        return df

    renamed = df
    for col_name in df.columns:
        if strategy == "snake_case":
            new_name = _to_snake_case(col_name)
        elif strategy == "lower":
            new_name = col_name.lower()
        else:
            # unknown strategy, keep as is
            new_name = col_name

        if new_name != col_name:
            renamed = renamed.withColumnRenamed(col_name, new_name)

    return renamed


# ---------- 2. APPLY CORRECT DATA TYPES ----------

def _parse_type(type_str: str) -> T.DataType:
    """
    Map simple config strings to Spark types.
    Supports:
      string, int, bigint, double, float, boolean,
      date, timestamp, decimal(p,s)
    """
    t = type_str.strip().lower()

    if t == "string":
        return T.StringType()
    if t in ("int", "integer"):
        return T.IntegerType()
    if t in ("bigint", "long"):
        return T.LongType()
    if t in ("double",):
        return T.DoubleType()
    if t in ("float",):
        return T.FloatType()
    if t in ("bool", "boolean"):
        return T.BooleanType()
    if t == "date":
        return T.DateType()
    if t in ("timestamp", "datetime"):
        return T.TimestampType()

    # decimal(18,2) etc.
    if t.startswith("decimal"):
        inside = t[t.find("(") + 1 : t.find(")")]
        precision_str, scale_str = inside.split(",")
        precision = int(precision_str)
        scale = int(scale_str)
        return T.DecimalType(precision=precision, scale=scale)

    # fallback
    return T.StringType()


def apply_column_types(
    df: DataFrame,
    column_types: Dict[str, str] | None
) -> DataFrame:
    """
    Casts columns to configured types.
    column_types:
      { "amount": "decimal(18,2)", "disbursement_dt": "date", ... }
    """
    if not column_types:
        return df

    casted = df
    for col_name, type_str in column_types.items():
        if col_name in casted.columns:
            spark_type = _parse_type(type_str)
            casted = casted.withColumn(col_name, F.col(col_name).cast(spark_type))

    return casted


# ---------- 3. ADD METADATA COLUMNS ----------

def add_metadata_columns(
    df: DataFrame,
    metadata_cfg: Dict[str, Any] | None
) -> DataFrame:
    """
    metadata_cfg:
      {
        "add_ingest_ts": true/false,
        "add_source_file": true/false,
        "add_batch_id": true/false
      }
    """
    if metadata_cfg is None:
        metadata_cfg = {}

    result = df

    if metadata_cfg.get("add_ingest_ts", False):
        result = result.withColumn("_ingest_ts", F.current_timestamp())

    if metadata_cfg.get("add_source_file", False):
        result = result.withColumn("_source_file", F.input_file_name())

    if metadata_cfg.get("add_batch_id", False):
        # simple UUID per run; if you want a global batch id, pass it in separately
        batch_id = str(uuid.uuid4())
        result = result.withColumn("_batch_id", F.lit(batch_id))

    return result


# ---------- MASTER FUNCTION: BRONZE TRANSFORM ----------

def apply_bronze_transformations(
    df: DataFrame,
    rules: Dict[str, Any]
) -> DataFrame:
    """
    Main Bronze transformation pipeline:
      1) Normalize column names
      2) Apply correct data types
      3) Add metadata columns
    """
    # 1) Normalize column names
    norm_strategy = rules.get("normalize_column_names")
    df_norm = normalize_column_names(df, norm_strategy)

    # 2) Apply data types
    column_types = rules.get("column_types")
    df_typed = apply_column_types(df_norm, column_types)

    # 3) Add metadata columns
    metadata_cfg = rules.get("metadata", {})
    df_meta = add_metadata_columns(df_typed, metadata_cfg)

    return df_meta
