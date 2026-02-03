# Databricks notebook source
from core.base_asset import BaseBronzeAsset
from core.base_asset import BaseSilverAsset
from transformations.silver_transformations import apply_silver_transformations

class DeltaAsset(BaseBronzeAsset):

    def read(self):
        print(f"[{self.name}] Reading Delta from: {self.source_path}")
        return self.spark.read.format("delta").load(self.source_path)

    def transform(self, df):
        print(f"[{self.name}] Applying Silver transformations")
        return apply_silver_transformations(df, self.transformations)

    def write(self, df):
        print(f"[{self.name}] Writing Silver Delta to: {self.target_path}")
        (
            df.write
            .format("delta")
            .mode(self.write_mode)
            .save(self.target_path)
        )


class DeltaAsset(BaseSilverAsset):
    
    def read(self):
        print(f"[{self.name}] Reading Delta from: {self.source_path}")
        return self.spark.read.format("delta").load(self.source_path)

    def transform(self, df):
        print(f"[{self.name}] Applying Silver transformations")
        return apply_silver_transformations(df, self.transformations)

    def write(self, df):
        print(f"[{self.name}] Writing Silver Delta to: {self.target_path}")
        (
            df.write
            .format("delta")
            .mode(self.write_mode)
            .save(self.target_path)
        )