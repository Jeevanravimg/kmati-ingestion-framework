# Databricks notebook source
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any

from pyspark.sql import DataFrame


class BaseBronzeAsset(ABC):
    """
    Base abstract class for all Bronze assets.
    Factory will return subclasses of this.
    """

    def __init__(self, spark, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.name = config.get("name", "unknown_asset")

    @abstractmethod
    def read(self) -> DataFrame:
        """Read raw data into a DataFrame."""
        ...

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Apply Bronze-level transformations."""
        ...

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """Write transformed data out (Delta files locally for now)."""
        ...

    def run(self) -> None:
        """Template method: read → transform → write."""
        print(f"\n=== Running Bronze asset: {self.name} ===")
        df_raw = self.read()
        df_bronze = self.transform(df_raw)
        self.write(df_bronze)
        print(f"=== Finished Bronze asset: {self.name} ===")

class BaseSilverAsset(ABC):
    """
    Base abstract class for all Silver assets.
    """

    def __init__(self, spark, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

        self.name = config.get("name", "unknown_asset")
        self.source_path = config.get("source_path")
        self.target_path = config.get("target_path")
        self.write_mode = config.get("write_mode", "overwrite")
        self.transformations = config.get("transformations", [])

    @abstractmethod
    def read(self) -> DataFrame:
        ...

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        ...

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        ...

    def run(self) -> None:
        print(f"\n=== Running Silver asset: {self.name} ===")
        df_raw = self.read()
        df_silver = self.transform(df_raw)
        self.write(df_silver)
        print(f"=== Finished Silver asset: {self.name} ===")
