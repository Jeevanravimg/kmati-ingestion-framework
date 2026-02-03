# Databricks notebook source

from __future__ import annotations
from typing import Dict, Any, List
import re

from pyspark.sql import DataFrame, functions as F

# ============================================================
# 1. TRIM STRINGS
# ============================================================
def trim_strings(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


# ============================================================
# 2. REGEX CLEAN
# ============================================================
def regex_clean(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("number_columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.regexp_replace(F.col(name), c["pattern_remove"], "")
            if c.get("trim"):
                expr_col = F.trim(expr_col)
            df = df.withColumn(c["out"], expr_col)

    for c in cfg.get("email_columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.col(name)
            if c.get("trim"):
                expr_col = F.trim(expr_col)
            if c.get("lowercase"):
                expr_col = F.lower(expr_col)
            df = df.withColumn(c["out"], expr_col)

    return df


# ============================================================
# 3. PARSE NUMERIC
# ============================================================
def parse_numeric(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            expr_col = F.regexp_replace(
                F.col(name).cast("string"),
                r"[^0-9\.\-]",
                ""
            )
            df = df.withColumn(c["out"], expr_col.cast(c["cast_type"]))
    return df


# ============================================================
# 4. PARSE DATE
# ============================================================
def parse_date(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            parsed = None
            for fmt in c.get("formats", []):
                attempt = F.to_date(F.col(name), fmt)
                parsed = attempt if parsed is None else F.coalesce(parsed, attempt)
            df = df.withColumn(c["out"], parsed)
    return df


# ============================================================
# 5. NORMALIZE STATUS
# ============================================================
def normalize_status(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            inp = F.lower(F.trim(F.col(name)))
            expr = None
            for raw, clean in c.get("mapping", {}).items():
                expr = (
                    F.when(inp == raw.lower(), F.lit(clean))
                    if expr is None
                    else expr.when(inp == raw.lower(), F.lit(clean))
                )
            df = df.withColumn(c["out"], expr.otherwise(inp))
    return df


# ============================================================
# 6. NULL HANDLING
# ============================================================
def null_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if cfg.get("enabled", False):
        df = df.dropna(subset=cfg.get("drop_if_null", []))
    return df


# ============================================================
# 7. DUPLICATE HANDLING
# ============================================================
def duplicate_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if cfg.get("enabled", False):
        df = df.dropDuplicates(cfg.get("unique_keys", []))
    return df


# ============================================================
# 8. RENAME COLUMNS
# ============================================================
def rename_columns(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for old, new in cfg.get("mapping", {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


# ============================================================
# 9. STANDARDIZE COLUMN CASE
# ============================================================
def standardize_column_case(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    def to_snake(name: str) -> str:
        n = name.lower()
        n = re.sub(r"[^\w]+", "_", n)
        n = re.sub(r"_+", "_", n)
        return n.strip("_")

    for c in df.columns:
        new = to_snake(c) if cfg.get("format") == "snake_case" else c.lower()
        if new != c:
            df = df.withColumnRenamed(c, new)

    return df


# ============================================================
# 10. FILL MISSING
# ============================================================
def fill_missing(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            df = df.withColumn(
                name,
                F.when(F.col(name).isNull(), F.lit(c["value"]))
                 .otherwise(F.col(name))
            )
    return df


# ============================================================
# 11. SURROGATE KEY
# ============================================================
def surrogate_key(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    col_name = cfg.get("column", "surrogate_key")
    df = df.withColumn(col_name, F.expr("uuid()"))
    return df


# ============================================================
# 12. OUTLIER CAPPING
# ============================================================
def outlier_capping(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        name = c["name"]
        if name in df.columns:
            df = df.withColumn(
                name,
                F.when(F.col(name) < c["min"], c["min"])
                 .when(F.col(name) > c["max"], c["max"])
                 .otherwise(F.col(name))
            )
    return df


# ============================================================
# 13. FUZZY NORMALIZE
# ============================================================
def fuzzy_normalize(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    rules = cfg.get("rules", {})

    for c in cfg.get("columns", []):
        name = c["name"]
        if name not in df.columns:
            continue

        expr = F.col(name)

        if rules.get("remove_punctuation"):
            expr = F.regexp_replace(expr, r"[^\w\s]", "")
        if rules.get("collapse_spaces"):
            expr = F.regexp_replace(expr, r"\s+", " ")
        if rules.get("lowercase"):
            expr = F.lower(expr)

        df = df.withColumn(c["out"], F.trim(expr))

    return df
from pyspark.sql.types import StructType, ArrayType

# ============================================================
# 14. FLATTEN JSON
# ============================================================
def flatten_json(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """
    Converts nested JSON (StructType & ArrayType) into flat structure.
    Mandatory for API / event-based JSON ingestion.
    """
    if not cfg.get("enabled", False):
        return df

    def _flatten(dframe: DataFrame) -> DataFrame:
        complex_fields = [
            (f.name, f.dataType)
            for f in dframe.schema.fields
            if isinstance(f.dataType, (StructType, ArrayType))
        ]

        while complex_fields:
            col_name, dtype = complex_fields.pop(0)

            # STRUCT → expand
            if isinstance(dtype, StructType):
                expanded = [
                    F.col(f"{col_name}.{f.name}").alias(f"{col_name}_{f.name}")
                    for f in dtype.fields
                ]
                dframe = dframe.select("*", *expanded).drop(col_name)

            # ARRAY → explode safely
            elif isinstance(dtype, ArrayType):
                dframe = dframe.withColumn(col_name, F.explode_outer(col_name))

            complex_fields = [
                (f.name, f.dataType)
                for f in dframe.schema.fields
                if isinstance(f.dataType, (StructType, ArrayType))
            ]

        return dframe

    return _flatten(df)

# ============================================================
# 15. SCHEMA VALIDATION
# ============================================================
def schema_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """
    Ensures mandatory JSON fields exist.
    Prevents silent schema drift issues.
    """
    required = cfg.get("required_columns", [])
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"❌ Missing required columns: {missing}")

    return df

# ============================================================
# 16. JSON KEY DEFAULT
# ============================================================
def json_key_default(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """
    Adds missing JSON keys with default values.
    """
    for col_cfg in cfg.get("columns", []):
        name = col_cfg["name"]
        default = col_cfg.get("default")

        if name not in df.columns:
            df = df.withColumn(name, F.lit(default))

    return df


# ============================================================
# 17. ARRAY SIZE VALIDATION
# ============================================================
def array_size_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    """
    Normalizes empty arrays before explode or joins.
    """
    for col_name in cfg.get("columns", []):
        if col_name in df.columns:
            df = df.withColumn(
                col_name,
                F.when(F.size(F.col(col_name)) == 0, None)
                 .otherwise(F.col(col_name))
            )
    return df


# ============================================================
# MASTER FUNCTION (ORDER IS EXPLICIT)
# ============================================================
def apply_silver_transformations(
    df: DataFrame,
    rules: List[Dict[str, Any]]
) -> DataFrame:
    """
    Order-wise Silver transformation pipeline
    """

    # ---- Validate unique order values ----
    orders = [t.get("order") for t in rules]
    if len(orders) != len(set(orders)):
        raise ValueError(f"Duplicate order values found: {orders}")

    # ---- Execute in order ----
    for tf in sorted(rules, key=lambda x: x.get("order", 0)):
        order = tf.get("order")

        if "trim_strings" in tf:
            print(f"➡ Order {order}: trim_strings")
            df = trim_strings(df, tf["trim_strings"])

        elif "regex_clean" in tf:
            print(f"➡ Order {order}: regex_clean")
            df = regex_clean(df, tf["regex_clean"])

        elif "parse_numeric" in tf:
            print(f"➡ Order {order}: parse_numeric")
            df = parse_numeric(df, tf["parse_numeric"])

        elif "parse_date" in tf:
            print(f"➡ Order {order}: parse_date")
            df = parse_date(df, tf["parse_date"])

        elif "normalize_status" in tf:
            print(f"➡ Order {order}: normalize_status")
            df = normalize_status(df, tf["normalize_status"])

        elif "null_handling" in tf:
            print(f"➡ Order {order}: null_handling")
            df = null_handling(df, tf["null_handling"])

        elif "duplicate_handling" in tf:
            print(f"➡ Order {order}: duplicate_handling")
            df = duplicate_handling(df, tf["duplicate_handling"])

        elif "rename_columns" in tf:
            print(f"➡ Order {order}: rename_columns")
            df = rename_columns(df, tf["rename_columns"])

        elif "standardize_column_case" in tf:
            print(f"➡ Order {order}: standardize_column_case")
            df = standardize_column_case(df, tf["standardize_column_case"])

        elif "fill_missing" in tf:
            print(f"➡ Order {order}: fill_missing")
            df = fill_missing(df, tf["fill_missing"])

        elif "surrogate_key" in tf:
            print(f"➡ Order {order}: surrogate_key")
            df = surrogate_key(df, tf["surrogate_key"])

        elif "outlier_capping" in tf:
            print(f"➡ Order {order}: outlier_capping")
            df = outlier_capping(df, tf["outlier_capping"])

        elif "fuzzy_normalize" in tf:
            print(f"➡ Order {order}: fuzzy_normalize")
            df = fuzzy_normalize(df, tf["fuzzy_normalize"])
        
        elif "flatten_json" in tf:
            print(f"➡ Order {order}: flatten_json")
            df = flatten_json(df, tf["flatten_json"])

        elif "schema_validation" in tf:
            print(f"➡ Order {order}: schema_validation")
            df = schema_validation(df, tf["schema_validation"])

        elif "json_key_default" in tf:
            print(f"➡ Order {order}: json_key_default")
            df = json_key_default(df, tf["json_key_default"])

        elif "array_size_validation" in tf:
            print(f"➡ Order {order}: array_size_validation")
            df = array_size_validation(df, tf["array_size_validation"])


    return df