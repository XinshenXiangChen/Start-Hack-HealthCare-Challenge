# SQL Server Target Schema

This folder contains SQL Server artifacts for target-table integration.

- `CreateImportTables.sql`: main target import schema used by the standardization pipeline.
- `CreateDatabase.cmd`: helper batch script for creating the database and tables on Windows with `sqlcmd`.

The pipeline is designed to emit standardized CSV files aligned with these table/column definitions.
