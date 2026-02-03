from __future__ import annotations

from typing import Dict, Any, List
import os
from pathlib import Path
import tempfile

import pandas as pd
from pyspark.sql import DataFrame

from .base_asset import BaseBronzeAsset
from transformations.bronze_transformations import apply_bronze_transformations


class LocalFileBronzeAsset(BaseBronzeAsset):
    """
    Local implementation that:
      - reads from local filesystem paths (folders or files)
      - supports multiple file formats
      - applies Bronze transformations
      - writes out as local Delta folders
    """

    # ---------- READ ----------

    def read(self) -> DataFrame:
        file_format = self.config["file_format"].lower()
        src_path = self.config["source_path"]
        options: Dict[str, Any] = self.config.get("options", {})

        print(f"[{self.name}] Reading format='{file_format}' from: {src_path}")

        if file_format in ("csv", "txt"):
            return self._read_csv_like(src_path, options)

        if file_format == "json":
            return self._read_json(src_path, options)

        if file_format == "parquet":
            return self._read_parquet(src_path)

        if file_format == "orc":
            return self._read_orc(src_path)

        if file_format == "avro":
            return self._read_avro(src_path, options)

        if file_format == "delta":
            return self._read_delta(src_path)

        if file_format == "excel":
            return self._read_excel(src_path, options)

        if file_format == "fixed_width":
            return self._read_fixed_width(src_path, options)

        raise ValueError(f"Unsupported file_format '{file_format}' for asset '{self.name}'")

    def _read_csv_like(self, src_path: str, options: Dict[str, Any]) -> DataFrame:
        reader = self.spark.read.format("csv")
        for k, v in options.items():
            reader = reader.option(k, str(v))
        return reader.load(src_path)

    def _read_json(self, src_path: str, options: Dict[str, Any]) -> DataFrame:
        reader = self.spark.read.format("json")
        for k, v in options.items():
            reader = reader.option(k, str(v))
        return reader.load(src_path)

    def _read_parquet(self, src_path: str) -> DataFrame:
        return self.spark.read.parquet(src_path)

    def _read_orc(self, src_path: str) -> DataFrame:
        return self.spark.read.orc(src_path)

    def _read_avro(self, src_path: str, options: Dict[str, Any]) -> DataFrame:
        # Requires spark-avro package when you start Spark
        reader = self.spark.read.format("avro")
        for k, v in options.items():
            reader = reader.option(k, str(v))
        return reader.load(src_path)

    def _read_delta(self, src_path: str) -> DataFrame:
        return self.spark.read.format("delta").load(src_path)

    def _read_excel(self, src_path: str, options: Dict[str, Any]) -> DataFrame:
        """
        Excel handled via pandas, but we avoid pandas->Spark createDataFrame
        (which uses PythonRDD and can crash). Instead:
          1) Read Excel into pandas
          2) Write to a temp CSV
          3) Read CSV with Spark (pure JVM path)
        options:
          - sheet_name (default: 0)
        """
        sheet_name = options.get("sheet_name", 0)

        if os.path.isdir(src_path):
            raise ValueError(
                f"[{self.name}] Excel format expects a single file, "
                f"but directory given: {src_path}"
            )

        print(f"[{self.name}] Using pandas to read Excel: {src_path}, sheet={sheet_name}")
        pdf = pd.read_excel(src_path, sheet_name=sheet_name, engine="openpyxl")

        # Write pandas DataFrame to a temp CSV
        tmp_dir = Path(tempfile.gettempdir()) / "bronze_excel_tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        tmp_csv = tmp_dir / f"{Path(src_path).stem}_{self.name}_tmp.csv"
        print(f"[{self.name}] Writing temp CSV for Spark read: {tmp_csv}")
        pdf.to_csv(tmp_csv, index=False)

        # Let Spark read the CSV (JVM only, no PythonRDD in write path)
        df = (
            self.spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")  # types will be refined by column_types rules
            .load(str(tmp_csv))
        )

        return df

    def _read_fixed_width(self, src_path: str, options: Dict[str, Any]) -> DataFrame:
        """
        Fixed-width text via pandas.read_fwf → Spark.
        options:
          - colspecs: list of (start, end) tuples
          - names: list of column names
        """
        colspecs: List[List[int]] = options.get("colspecs")
        names: List[str] = options.get("names")

        if colspecs is None or names is None:
            raise ValueError(
                f"[{self.name}] fixed_width requires 'colspecs' and 'names' in options"
            )

        if os.path.isdir(src_path):
            raise ValueError(
                f"[{self.name}] fixed_width expects a single file, not a directory: {src_path}"
            )

        print(f"[{self.name}] Using pandas.read_fwf for fixed-width file: {src_path}")
        pdf = pd.read_fwf(src_path, colspecs=colspecs, names=names)
        return self.spark.createDataFrame(pdf)

    # ---------- TRANSFORM ----------

    def transform(self, df: DataFrame) -> DataFrame:
        rules = self.config.get("transformations", {})
        print(f"[{self.name}] Applying Bronze transformations: {rules}")
        return apply_bronze_transformations(df, rules)

    # ---------- WRITE ----------

    def write(self, df: DataFrame) -> None:
        target_path = self.config["target_path"]
        mode = self.config.get("write_mode", "append")

        print(f"[{self.name}] Writing local Delta to: {target_path} (mode={mode})")
        (
            df.write
            .format("delta")
            .mode(mode)
            .save(target_path)
        )
