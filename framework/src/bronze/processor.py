import re, uuid
from typing import Dict, Any
from pyspark.sql import SparkSession, functions as F


def process_bronze_asset(spark: SparkSession, asset: Dict[str, Any]) -> None:
    df = (
        spark.read.csv(asset["source_path"], header=True, inferSchema=True)
        if asset.get("file_format", "csv") == "csv"
        else spark.read.parquet(asset["source_path"])
    )

    for col, typ in asset.get("transformations", {}).get("column_types", {}).items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(typ))

    if asset.get("transformations", {}).get("normalize_column_names") == "snake_case":
        for c in df.columns:
            new = re.sub(r"[^\w]+", "_", c.lower()).strip("_")
            df = df.withColumnRenamed(c, new)

    meta = asset.get("transformations", {}).get("metadata", {})
    if meta.get("add_ingest_ts"):
        df = df.withColumn("_ingest_ts", F.current_timestamp())
    if meta.get("add_batch_id"):
        df = df.withColumn("_batch_id", F.lit(str(uuid.uuid4())[:8]))

    df.write.mode(asset.get("write_mode", "overwrite")).parquet(asset["target_path"])
