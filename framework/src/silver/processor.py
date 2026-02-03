# from typing import Dict, Any, List
# from pyspark.sql import SparkSession
# from .transformations import *


# def apply_silver_transformations(df, rules: List[Dict[str, Any]]):
#     orders = [r["order"] for r in rules]
#     if len(orders) != len(set(orders)):
#         raise ValueError("Duplicate transformation order")

#     for r in sorted(rules, key=lambda x: x["order"]):
#         if "trim_strings" in r:
#             df = trim_strings(df, r["trim_strings"])
#         elif "regex_clean" in r:
#             df = regex_clean(df, r["regex_clean"])
#         elif "parse_numeric" in r:
#             df = parse_numeric(df, r["parse_numeric"])
#         elif "parse_date" in r:
#             df = parse_date(df, r["parse_date"])
#         elif "normalize_status" in r:
#             df = normalize_status(df, r["normalize_status"])
#         elif "null_handling" in r:
#             df = null_handling(df, r["null_handling"])
#         elif "duplicate_handling" in r:
#             df = duplicate_handling(df, r["duplicate_handling"])
#         elif "rename_columns" in r:
#             df = rename_columns(df, r["rename_columns"])
#         elif "standardize_column_case" in r:
#             df = standardize_column_case(df, r["standardize_column_case"])
#         elif "fill_missing" in r:
#             df = fill_missing(df, r["fill_missing"])
#         elif "surrogate_key" in r:
#             df = surrogate_key(df, r["surrogate_key"])
#         elif "outlier_capping" in r:
#             df = outlier_capping(df, r["outlier_capping"])
#         elif "fuzzy_normalize" in r:
#             df = fuzzy_normalize(df, r["fuzzy_normalize"])
#         elif "flatten_json" in r:
#             df = flatten_json(df, r["flatten_json"])
#         elif "schema_validation" in r:
#             df = schema_validation(df, r["schema_validation"])
#         elif "json_key_default" in r:
#             df = json_key_default(df, r["json_key_default"])
#         elif "array_size_validation" in r:
#             df = array_size_validation(df, r["array_size_validation"])
#     return df


# def process_silver_asset(spark: SparkSession, asset: Dict[str, Any]) -> None:
#     df = spark.read.parquet(asset["source_path"])
#     df = apply_silver_transformations(df, asset.get("transformations", []))
#     df.write.mode(asset.get("write_mode", "overwrite")).format("delta").saveAsTable(asset["target_table"])
from typing import Dict, Any, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from .transformations import *


def apply_silver_transformations(df, rules: List[Dict[str, Any]]):
    orders = [r["order"] for r in rules]
    if len(orders) != len(set(orders)):
        raise ValueError("Duplicate transformation order")

    for r in sorted(rules, key=lambda x: x["order"]):
        if "trim_strings" in r:
            df = trim_strings(df, r["trim_strings"])
        elif "regex_clean" in r:
            df = regex_clean(df, r["regex_clean"])
        elif "parse_numeric" in r:
            df = parse_numeric(df, r["parse_numeric"])
        elif "parse_date" in r:
            df = parse_date(df, r["parse_date"])
        elif "normalize_status" in r:
            df = normalize_status(df, r["normalize_status"])
        elif "null_handling" in r:
            df = null_handling(df, r["null_handling"])
        elif "duplicate_handling" in r:
            df = duplicate_handling(df, r["duplicate_handling"])
        elif "rename_columns" in r:
            df = rename_columns(df, r["rename_columns"])
        elif "standardize_column_case" in r:
            df = standardize_column_case(df, r["standardize_column_case"])
        elif "fill_missing" in r:
            df = fill_missing(df, r["fill_missing"])
        elif "surrogate_key" in r:
            df = surrogate_key(df, r["surrogate_key"])
        elif "outlier_capping" in r:
            df = outlier_capping(df, r["outlier_capping"])
        elif "fuzzy_normalize" in r:
            df = fuzzy_normalize(df, r["fuzzy_normalize"])
        elif "flatten_json" in r:
            df = flatten_json(df, r["flatten_json"])
        elif "schema_validation" in r:
            df = schema_validation(df, r["schema_validation"])
        elif "json_key_default" in r:
            df = json_key_default(df, r["json_key_default"])
        elif "array_size_validation" in r:
            df = array_size_validation(df, r["array_size_validation"])

    return df


def process_silver_asset(spark: SparkSession, asset: Dict[str, Any]) -> None:
    # Read from Bronze
    df = spark.read.parquet(asset["source_path"])

    # Apply transformations
    df = apply_silver_transformations(df, asset.get("transformations", []))

    # 🔒 SCHEMA ENFORCEMENT (critical fix)
    # Ensure column types never change across runs
    if "day_raw" in df.columns:
        df = df.withColumn("day_raw", col("day_raw").cast("date"))
        # 👉 change to "string" if that is your canonical type

    # Write to Silver Delta table
    (
        df.write
          .format("delta")
          .mode(asset.get("write_mode", "overwrite"))
          .option("overwriteSchema", "true")  # safe with explicit casting
          .saveAsTable(asset["target_table"])
    )
