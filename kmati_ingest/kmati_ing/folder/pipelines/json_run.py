# Databricks notebook source
from __future__ import annotations

# ---------------------------------------------------------
# FIX PYTHON PATH
# ---------------------------------------------------------
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------
# IMPORTS
# ---------------------------------------------------------
import yaml
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from core.asset_factory import BronzeAssetFactory
from core.silver_asset_factory import SilverAssetFactory


# ---------------------------------------------------------
# LOAD CONFIG
# ---------------------------------------------------------
def load_config(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------
# CREATE SPARK SESSION
# ---------------------------------------------------------
def create_spark():
    builder = (
        SparkSession.builder
        .appName("MedallionPipeline")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.hadoop.use.windows.native.io", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    config_path = PROJECT_ROOT / "configs" / "json_config.yaml"

    print(f"[runner] Project root : {PROJECT_ROOT}")
    print(f"[runner] Config file  : {config_path}")

    spark = create_spark()

    try:
        cfg = load_config(config_path)
        assets = cfg.get("assets", [])

        for asset_cfg in assets:
            layer = asset_cfg.get("layer")

            if layer == "bronze":
                asset = BronzeAssetFactory.create(spark, asset_cfg)

            elif layer == "silver":
                asset = SilverAssetFactory.create(spark, asset_cfg)

            else:
                raise ValueError(
                    f"Unknown layer '{layer}' for asset '{asset_cfg.get('name')}'"
                )

            asset.run()

    finally:
        print("[runner] Stopping SparkSession")
        spark.stop()