from typing import Dict, Any
import re
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType


def trim_strings(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df
    for c in cfg.get("columns", []):
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def regex_clean(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("number_columns", []):
        if c["name"] in df.columns:
            expr = F.regexp_replace(F.col(c["name"]), c["pattern_remove"], "")
            if c.get("trim"):
                expr = F.trim(expr)
            df = df.withColumn(c["out"], expr)

    for c in cfg.get("email_columns", []):
        if c["name"] in df.columns:
            expr = F.col(c["name"])
            if c.get("trim"):
                expr = F.trim(expr)
            if c.get("lowercase"):
                expr = F.lower(expr)
            df = df.withColumn(c["out"], expr)

    return df


def parse_numeric(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            expr = F.regexp_replace(F.col(c["name"]).cast("string"), r"[^0-9\.\-]", "")
            df = df.withColumn(c["out"], expr.cast(c["cast_type"]))
    return df


def parse_date(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            parsed = None
            for fmt in c.get("formats", []):
                attempt = (
                    F.to_date(F.to_timestamp(F.col(c["name"]), fmt))
                    if any(t in fmt for t in ["HH", "hh", "mm", "ss"])
                    else F.to_date(F.col(c["name"]), fmt)
                )
                parsed = attempt if parsed is None else F.coalesce(parsed, attempt)
            df = df.withColumn(c["out"], parsed)
    return df


def normalize_status(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            inp = F.lower(F.trim(F.col(c["name"])))
            expr = None
            for raw, clean in c.get("mapping", {}).items():
                expr = (
                    F.when(inp == raw.lower(), F.lit(clean))
                    if expr is None
                    else expr.when(inp == raw.lower(), F.lit(clean))
                )
            df = df.withColumn(c["out"], expr.otherwise(inp))
    return df


def null_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    return df.dropna(subset=cfg.get("drop_if_null", [])) if cfg.get("enabled") else df


def duplicate_handling(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    return df.dropDuplicates(cfg.get("unique_keys", [])) if cfg.get("enabled") else df


def rename_columns(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df
    for old, new in cfg.get("mapping", {}).items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def standardize_column_case(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    def to_snake(name: str) -> str:
        n = re.sub(r"[^\w]+", "_", name.lower())
        return re.sub(r"_+", "_", n).strip("_")

    for c in df.columns:
        new = to_snake(c) if cfg.get("format") == "snake_case" else c.lower()
        if new != c:
            df = df.withColumnRenamed(c, new)
    return df


def fill_missing(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df
    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            df = df.withColumn(
                c["name"],
                F.when(F.col(c["name"]).isNull(), F.lit(c["value"])).otherwise(F.col(c["name"]))
            )
    return df


def surrogate_key(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    return df.withColumn(cfg.get("column", "surrogate_key"), F.expr("uuid()")) if cfg.get("enabled") else df


def outlier_capping(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df
    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            df = df.withColumn(
                c["name"],
                F.when(F.col(c["name"]) < c["min"], c["min"])
                 .when(F.col(c["name"]) > c["max"], c["max"])
                 .otherwise(F.col(c["name"]))
            )
    return df


def fuzzy_normalize(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    rules = cfg.get("rules", {})
    for c in cfg.get("columns", []):
        if c["name"] in df.columns:
            expr = F.col(c["name"])
            if rules.get("remove_punctuation"):
                expr = F.regexp_replace(expr, r"[^\w\s]", "")
            if rules.get("collapse_spaces"):
                expr = F.regexp_replace(expr, r"\s+", " ")
            if rules.get("lowercase"):
                expr = F.lower(expr)
            df = df.withColumn(c["out"], F.trim(expr))
    return df


def flatten_json(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    if not cfg.get("enabled", False):
        return df

    complex_fields = [(f.name, f.dataType) for f in df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]

    while complex_fields:
        col, dtype = complex_fields.pop(0)
        if isinstance(dtype, StructType):
            expanded = [F.col(f"{col}.{f.name}").alias(f"{col}_{f.name}") for f in dtype.fields]
            df = df.select("*", *expanded).drop(col)
        else:
            df = df.withColumn(col, F.explode_outer(col))
        complex_fields = [(f.name, f.dataType) for f in df.schema.fields if isinstance(f.dataType, (StructType, ArrayType))]
    return df


def schema_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    missing = [c for c in cfg.get("required_columns", []) if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    return df


def json_key_default(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    for c in cfg.get("columns", []):
        if c["name"] not in df.columns:
            df = df.withColumn(c["name"], F.lit(c.get("default")))
    return df


def array_size_validation(df: DataFrame, cfg: Dict[str, Any]) -> DataFrame:
    for col in cfg.get("columns", []):
        if col in df.columns:
            df = df.withColumn(col, F.when(F.size(F.col(col)) == 0, None).otherwise(F.col(col)))
    return df
