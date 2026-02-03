from __future__ import annotations

from typing import Dict, Any

from pyspark.sql import SparkSession

from .base_asset import BaseBronzeAsset
from .file_asset import LocalFileBronzeAsset


class BronzeAssetFactory:
    """
    Factory that builds a Bronze asset object based on config.
    """

    @staticmethod
    def create(
        spark: SparkSession,
        asset_config: Dict[str, Any]
    ) -> BaseBronzeAsset:
        asset_type = asset_config.get("asset_type")

        if asset_type == "local_file":
            return LocalFileBronzeAsset(spark, asset_config)

        # Later:
        # if asset_type == "volume_file": return VolumeFileBronzeAsset(...)
        # if asset_type == "jdbc_source": return JdbcBronzeAsset(...)

        raise ValueError(
            f"Unknown asset_type '{asset_type}' for asset '{asset_config.get('name')}'"
        )
