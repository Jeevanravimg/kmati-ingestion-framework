# Databricks notebook source
from core.delta_asset import DeltaAsset

class SilverAssetFactory:

    @staticmethod
    def create(spark, cfg):
        asset_type = cfg.get("asset_type")
        name = cfg.get("name", "<no-name>")

        if asset_type == "delta_table":
            return DeltaAsset(spark, cfg)

        raise ValueError(
            f"Unknown asset_type '{asset_type}' for asset '{name}'"
        )
