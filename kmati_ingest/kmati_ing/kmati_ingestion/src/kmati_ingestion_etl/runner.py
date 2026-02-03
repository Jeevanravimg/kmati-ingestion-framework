from __future__ import annotations

import yaml
from pyspark.sql import SparkSession
from typing import Dict, Any, List

from transformations.bronze_transformations import apply_bronze_transformations
from transformations.silver_transformations import apply_silver_transformations


# =====================================================
# UTILS
# =====================================================
def _require_key(obj: Dict[str, Any], key: str, ctx: str):
    if key not in obj:
        raise ValueError(f"❌ Missing required key '{key}' in {ctx}")


# =====================================================
# BRONZE PROCESSING
# =====================================================
def run_bronze(spark: SparkSession, assets: List[Dict[str, Any]]) -> None:
    if not assets:
        print("ℹ No BRONZE assets defined. Skipping Bronze layer.")
        return

    for asset in assets:
        _require_key(asset, "name", "bronze asset")
        _require_key(asset, "file_format", f"bronze asset {asset['name']}")
        _require_key(asset, "source_path", f"bronze asset {asset['name']}")
        _require_key(asset, "target_table", f"bronze asset {asset['name']}")

        print(f"🚀 BRONZE | {asset['name']}")

        df = (
            spark.read
            .format(asset["file_format"])
            .options(**asset.get("options", {}))
            .load(asset["source_path"])   # ✅ Databricks Volume
        )

        df_bronze = apply_bronze_transformations(
            df,
            asset.get("transformations", {})
        )

        (
            df_bronze
            .write
            .mode(asset.get("write_mode", "overwrite"))
            .format("delta")
            .saveAsTable(asset["target_table"])   # ✅ catalog.schema.table
        )

        print(f"✅ Bronze table created: {asset['target_table']}")


# =====================================================
# SILVER PROCESSING
# =====================================================
def run_silver(spark: SparkSession, assets: List[Dict[str, Any]]) -> None:
    if not assets:
        print("ℹ No SILVER assets defined. Skipping Silver layer.")
        return

    for asset in assets:
        _require_key(asset, "name", "silver asset")
        _require_key(asset, "source_table", f"silver asset {asset['name']}")
        _require_key(asset, "target_table", f"silver asset {asset['name']}")

        print(f"🚀 SILVER | {asset['name']}")

        df = spark.table(asset["source_table"])   # ✅ Read Bronze Delta table

        df_silver = apply_silver_transformations(
            df,
            asset.get("transformations", [])
        )

        (
            df_silver
            .write
            .mode(asset.get("write_mode", "overwrite"))
            .format("delta")
            .saveAsTable(asset["target_table"])
        )

        print(f"✅ Silver table created: {asset['target_table']}")


# =====================================================
# ENTRY POINT (USED BY DATABRICKS JOB)
# =====================================================
def run(config_path: str) -> None:
    spark = SparkSession.builder.getOrCreate()

    print(f"📄 Loading pipeline config: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # ----------------------------
    # Validate top-level structure
    # ----------------------------
    _require_key(config, "execution", "root config")
    _require_key(config, "assets", "root config")

    execution = config["execution"]
    assets = config["assets"]

    order = execution.get("order", [])
    if not order:
        raise ValueError("❌ execution.order is empty or missing")

    print(f"🧭 Execution order: {order}")

    # ----------------------------
    # Execute layers in order
    # ----------------------------
    if "bronze" in order:
        run_bronze(spark, assets.get("bronze", []))

    if "silver" in order:
        run_silver(spark, assets.get("silver", []))

    print("🎉 Bronze → Silver pipeline completed successfully")
