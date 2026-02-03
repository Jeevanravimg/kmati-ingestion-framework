# Databricks notebook source
from __future__ import annotations

import sys
from pathlib import Path
import yaml

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

# -------------------------------------------------
# FIX PROJECT PATH
# -------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# -------------------------------------------------
# IMPORT SCD FUNCTIONS
# -------------------------------------------------
from scd.scd1 import scd1
from scd.scd2 import scd2

# -------------------------------------------------
# SPARK SESSION
# -------------------------------------------------
def create_spark():
    builder = (
        SparkSession.builder
        .appName("SCD Runner")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# -------------------------------------------------
# LOAD YAML CONFIG
# -------------------------------------------------
def load_config(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# -------------------------------------------------
# MAIN DRIVER
# -------------------------------------------------
if __name__ == "__main__":

    spark = create_spark()

    config_path = PROJECT_ROOT / "configs" / "scd_config.yaml"
    cfg = load_config(config_path)

    for table in cfg["scd_tables"]:

        table_name = table["table_name"]
        source_path = table["source_path"]
        target_path = table["target_path"]
        scd_cfg = table["scd"]

        print(f"\n Processing table: {table_name}")

        # ----------------------------
        # READ SOURCE
        # ----------------------------
        df = spark.read.format("delta").load(source_path)

        # ----------------------------
        # INITIAL LOAD
        # ----------------------------
        if not DeltaTable.isDeltaTable(spark, target_path):

            if scd_cfg["type"] == "scd2":
                cols = scd_cfg["scd2_columns"]
                df = (
                    df.withColumn(cols["start_date"], current_date())
                      .withColumn(cols["end_date"], lit(None))
                      .withColumn(cols["is_current"], lit(True))
                )

            df.write.format("delta").mode("overwrite").save(target_path)
            print("Initial load completed")
            continue

        # ----------------------------
        # APPLY SCD LOGIC
        # ----------------------------
        if scd_cfg["type"] == "scd1":
            scd1(df, scd_cfg)

        elif scd_cfg["type"] == "scd2":
            scd2(df, scd_cfg)

        elif scd_cfg["type"] == "none":
            (
                df.filter("op = 'INSERT'")
                  .drop("op")
                  .write
                  .format("delta")
                  .mode("append")
                  .save(target_path)
            )
            print("➡ Fact CDC append completed")

        else:
            raise ValueError(f"Invalid SCD type: {scd_cfg['type']}")

        print(f"Completed {table_name}")

    spark.stop()

# from __future__ import annotations

# import sys
# from pathlib import Path
# import yaml
# from pyspark.sql import SparkSession
# from delta import configure_spark_with_delta_pip
# from delta.tables import DeltaTable
# from pyspark.sql.functions import current_date, lit

# # -------------------------------------------------
# # FIX PYTHON PATH
# # -------------------------------------------------
# PROJECT_ROOT = Path(__file__).resolve().parents[1]
# sys.path.insert(0, str(PROJECT_ROOT))

# # -------------------------------------------------
# # IMPORT SCD MODULES (✅ FIXED)
# # -------------------------------------------------
# from scd.scd1 import scd1
# from scd.scd2 import scd2

# # -------------------------------------------------
# # SPARK
# # -------------------------------------------------
# def create_spark():
#     builder = (
#         SparkSession.builder
#         .appName("SCD Runner")
#         .master("local[*]")
#         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
#         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
#     )
#     return configure_spark_with_delta_pip(builder).getOrCreate()

# # -------------------------------------------------
# # MAIN
# # -------------------------------------------------
# if __name__ == "__main__":

#     spark = create_spark()

#     config_path = PROJECT_ROOT / "configs" / "scd_config.yaml"
#     with open(config_path, "r") as f:
#         cfg = yaml.safe_load(f)

#     source_path = cfg["asset"]["source_path"]
#     target_path = cfg["asset"]["target_path"]
#     scd_cfg = cfg["scd"]

#     # ----------------------------
#     # READ SOURCE
#     # ----------------------------
#     df = spark.read.format("delta").load(source_path)

#     # ----------------------------
#     # FIRST LOAD
#     # ----------------------------
#     if not DeltaTable.isDeltaTable(spark, target_path):

#         if scd_cfg["type"] == "scd2":
#             cols = scd_cfg["scd2_columns"]
#             df = (
#                 df.withColumn(cols["start_date"], current_date())
#                   .withColumn(cols["end_date"], lit(None))
#                   .withColumn(cols["is_current"], lit(True))
#             )

#         df.write.format("delta").mode("overwrite").save(target_path)
#         print("✅ Initial load completed")
#         spark.stop()
#         sys.exit(0)

#     # ----------------------------
#     # APPLY SCD
#     # ----------------------------
#     if scd_cfg["type"] == "scd1":
#         scd1(df, scd_cfg)

#     elif scd_cfg["type"] == "scd2":
#         scd2(df, scd_cfg)

#     else:
#         raise ValueError("Invalid SCD type")

#     spark.stop()