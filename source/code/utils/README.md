# utils — Shared Utility Functions and Configuration

This folder contains shared modules and configuration helpers used across the ETL pipeline. It centralizes database types, environment variables, reusable logic, and path configs to keep all scripts consistent and DRY.

## Files

### `db_types.py` 

Centralized module for importing and exposing commonly used SQLAlchemy data types, including PostgreSQL-specific types like `JSONB` and `ARRAY`. Ensures consistency across all schema definitions.

### `path_config.py` 

Defines and exports shared directory paths (e.g., for JSON, Excel, or downloaded files) used across the project. Keeps file path management organized and consistent.

### `orchestrator.py`

This file provides core ETL operations and helpers used across the entire codebase:

- **`get_data_from_db()`**: Executes a SQL query with retry and timeout logic, returning the result as a list of dictionaries.
- **`ensure_table_structure()`**: Ensures a schema/table exists and validates its structure against `fields_dict`. Creates the schema/table if permitted.
- **`update_insert_dw()`**: Performs an UPSERT into the target table using PostgreSQL's `ON CONFLICT` clause. Supports handling JSON fields and nulls.
- **`load_json_file()`**: Loads JSON files from disk and validates the structure.

These functions form the backbone of all ETL scripts.

### `env.py`

Handles centralized loading and validation of all sensitive environment variables.

- Loads credentials and connection details for PostgreSQL, BESYS, NOVA, and DATAFORDELER.
- Uses `python-dotenv` to load from a `.env` file and enforces presence with `assert` checks.
- Example variables:
  - `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USERNAME`, `POSTGRES_PASSWORD` etc.

This ensures all credentials are available at runtime and reduces the risk of silent misconfigurations.

### Usage

In ETL scripts, these are commonly used like so:

```python
from utils.orchestrator import get_data_from_db, ensure_table_structure, update_insert_dw
from utils.env import POSTGRES_USERNAME
```

This modular design allows each script to focus only on domain logic while reusing robust shared functionality.

