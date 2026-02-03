# KMATI Ingestion Framework  
### Databricks Asset Bundle–Based Automated Medallion Data Pipeline

## Overview

The **KMATI Ingestion Framework** is a reusable, configuration-driven **automated data pipeline generator** built on the **Databricks Lakehouse platform** using **Databricks Asset Bundles**.

The framework enables teams to **ingest, standardize, and transform data into Bronze and Silver layers** by simply updating configuration files—without rewriting pipeline code.  
It is designed for **enterprise-scale ingestion**, **schema safety**, and **production-grade automation**.

---

## What This Framework Does

- Automates **end-to-end data ingestion pipelines**
- Generates **Bronze and Silver layer transformations dynamically**
- Executes pipelines using a **single orchestrated entry point**
- Runs as a **deployable Databricks Asset Bundle**
- Supports **repeatable, testable, and environment-safe deployments**

This framework is ideal for organizations that want to **standardize ingestion**, **reduce manual coding**, and **accelerate onboarding of new data sources**.

---

## High-Level Architecture

1. **Configuration-Driven Execution**
   - All pipeline behavior is controlled via YAML configuration files.
   - Adding a new dataset requires configuration changes only.

2. **Wheel-Based Execution Model**
   - The framework is packaged as a Python wheel.
   - The wheel is downloaded and executed from a Databricks volume.
   - Ensures versioned, repeatable, and controlled execution.

3. **Medallion Architecture**
   - **Bronze Layer**: Raw ingestion with metadata enrichment and standardization.
   - **Silver Layer**: Cleaned, validated, business-ready datasets with enforced schemas.

---

## Execution Flow

1. Databricks job starts the **main pipeline entry point**.
2. Configuration file path is passed as a runtime parameter.
3. The framework reads the execution order and asset definitions.
4. Bronze assets are ingested from source locations.
5. Silver transformations are applied sequentially based on rules.
6. Final outputs are stored as managed Delta tables in the catalog.

This allows **multiple pipelines** to be generated and executed using the same framework.

---

## Key Capabilities

### Bronze Layer
- Supports CSV and Parquet sources
- Automatic column type casting
- Column name normalization
- Ingestion metadata (timestamp, batch ID)
- Configurable write modes

### Silver Layer
- Ordered, rule-based transformations
- Schema enforcement to prevent drift
- Data quality handling:
  - Null handling
  - Duplicate removal
  - Standardization
  - Outlier capping
  - JSON flattening
  - Surrogate key generation
- Outputs stored as Delta tables

---

## Why This Framework Is Valuable

- **Automation First**: Pipelines are generated automatically from configuration.
- **Low Code**: No pipeline rewrites when new datasets are added.
- **Safe by Design**: Schema enforcement prevents silent data corruption.
- **Reusable**: Same framework supports multiple domains and use cases.
- **Production Ready**: Deployed, versioned, and executed using Asset Bundles.

---

## Deployment & Execution

- Built and packaged as a Python wheel
- Deployed using **Databricks Asset Bundles**
- Executed via Databricks Jobs
- Supports environment-specific targets (dev, prod)
- Fully compatible with Unity Catalog governance

---

## Use Cases

- Enterprise ingestion standardization
- Rapid onboarding of new data sources
- Bronze–Silver pipeline automation
- Data platform foundation for analytics and ML
- Scalable ingestion frameworks for multi-team environments

---

## Summary

The **KMATI Ingestion Framework** provides a **scalable, configurable, and production-grade solution** for building medallion-based data pipelines on Databricks.  
By separating **pipeline logic from configuration**, it enables faster delivery, safer deployments, and consistent data quality across the organization.

## Author

**Jeevan M G**  
Data Engineer 
