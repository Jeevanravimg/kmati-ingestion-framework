# Databricks notebook source
from datetime import datetime

from src.common.config import load_config
from src.bronze.processor import process_bronze_asset
from src.silver.processor import process_silver_asset

# Job parameter
dbutils.widgets.text("config_path", "")
config_path = dbutils.widgets.get("config_path")

print("🚀 PIPELINE STARTED")
print("Config path:", config_path)
print("Start time:", datetime.now())

# Load config (path is resolved by the bundle runtime)
config = load_config(config_path)

for layer in config["execution"]["order"]:
    for asset in config["assets"].get(layer, []):
        if layer == "bronze":
            process_bronze_asset(spark, asset)
        elif layer == "silver":
            process_silver_asset(spark, asset)

print("✅ PIPELINE COMPLETED")
print("End time:", datetime.now())
