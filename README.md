****# Postgres Diff Inspector (PDI)

A powerful CLI tool for schema and data comparison between PostgreSQL databases.

## Features

- Schema comparison between two PostgreSQL databases
- Data comparison between two PostgreSQL databases
- **PostgreSQL dump file comparison** ✨ **NEW**
- **Mixed comparison (live DB vs dump file)** ✨ **NEW**
- **Automatic missing table creation** ✨ **NEW**
- **CREATE TABLE SQL generation** ✨ **NEW**
- Missing record detection
- Automatic INSERT SQL generation
- Automatic missing record insertion
- Bidirectional comparison (Cloud ↔ Edge)
- Organized output directories
- Detailed table structure analysis
- Column types, constraints, and index comparison
- SSL/TLS connection support
- Colorful terminal output
- Detailed reporting

## Installation

```bash
npm install
```

## Usage

### Basic Usage

#### Schema Comparison

##### Live Databases
```bash
pdi schema \
  --cloud "postgresql://user:pass@localhost:5432/db1" \
  --edge "postgresql://user:pass@cloud-host:5432/db2" \
  --schema public
```

##### Dump Files
```bash
pdi schema \
  --cloud-dump cloud_backup.sql \
  --edge-dump edge_backup.sql \
  --schema public
```

##### Mixed (DB + Dump)
```bash
pdi schema \
  --cloud "postgresql://user:pass@localhost:5432/db1" \
  --edge-dump edge_backup.sql \
  --schema public
```

#### Data Comparison

##### Live Databases
```bash
pdi records \
  --cloud "postgresql://user:pass@localhost:5432/db1" \
  --edge "postgresql://user:pass@cloud-host:5432/db2" \
  --schema public
```

##### Dump Files
```bash
pdi records \
  --cloud-dump cloud_backup.sql \
  --edge-dump edge_backup.sql \
  --schema public
```

##### Mixed (DB + Dump)
```bash
pdi records \
  --cloud "postgresql://user:pass@localhost:5432/db1" \
  --edge-dump edge_backup.sql \
  --schema public
```

### Parameters

#### Schema Comparison
- `--cloud, -c`: Cloud database connection URL
- `--edge, -e`: Edge database connection URL
- `--cloud-dump`: Cloud database dump file
- `--edge-dump`: Edge database dump file
- `--schema, -s`: Schema name to compare (default: public)
- `--output, -o`: Output file (optional)
- `--verbose, -v`: Verbose output
- `--execute`: Automatically create missing tables ✨ **NEW**
- `--dry-run`: Preview SQL execution without running ✨ **NEW**

#### Data Comparison
- `--cloud, -c`: Cloud database connection URL
- `--edge, -e`: Edge database connection URL
- `--cloud-dump`: Cloud database dump file
- `--edge-dump`: Edge database dump file
- `--schema, -s`: Schema name to compare (default: public)
- `--output, -o`: SQL output file (optional)
- `--verbose, -v`: Verbose output
- `--execute`: Automatically insert missing records
- `--dry-run`: Preview SQL execution without running

### Examples

#### Schema Comparison
```bash
# Live database comparison
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb"

# Dump file comparison
pdi schema --cloud-dump local_backup.sql --edge-dump cloud_backup.sql

# Mixed comparison (DB vs Dump)
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" --edge-dump cloud_backup.sql

# Specific schema comparison
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" -s "my_schema"

# Verbose output
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" -v

# Automatically create missing tables
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" --execute

# Dry run (test mode)
pdi schema -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" --execute --dry-run
```

#### Data Comparison
```bash
# Live database data comparison
pdi records -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb"

# Dump file data comparison
pdi records --cloud-dump local_backup.sql --edge-dump cloud_backup.sql

# Mixed data comparison (DB vs Dump)
pdi records -c "postgresql://user:pass@localhost:5432/localdb" --edge-dump cloud_backup.sql

# Automatic insertion (use with caution!)
pdi records -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" --execute

# Dry run (test mode)
pdi records -c "postgresql://user:pass@localhost:5432/localdb" -e "postgresql://user:pass@cloud.com:5432/clouddb" --execute --dry-run
```

#### Connection Health Check
```bash
# Test database connection
pdi health -u "postgresql://user:pass@localhost:5432/database"
```

## Output Format

### Schema Comparison
The tool generates detailed reports showing:
- Table structure differences
- Missing tables in each database
- Column type differences
- Constraint differences
- Index differences

### Data Comparison
The tool generates:
- Missing record counts per table
- INSERT SQL statements for synchronization
- Organized output directories:
  - `cloud-to-edge/`: Records to insert into Edge from Cloud
  - `edge-to-cloud/`: Records to insert into Cloud from Edge

### Organized Output Structure
```
output/
├── cloud-to-edge/
│   ├── missing-tables.sql    # For schema comparison
│   ├── missing-records.sql   # For data comparison
│   └── report.json
├── edge-to-cloud/
│   ├── missing-tables.sql    # For schema comparison
│   ├── missing-records.sql   # For data comparison
│   └── report.json
└── summary-report.json
```

## Environment Variables

You can create a `.env` file for frequently used connection strings and default values:

```bash
# Database Connection URLs
CLOUD_DB_URL=postgresql://username:password@cloud-host:5432/database_name
EDGE_DB_URL=postgresql://username:password@edge-host:5432/database_name

# Default schema name
DEFAULT_SCHEMA=public

# Output directory
OUTPUT_DIR=output
```

### Using Environment Variables

When you set environment variables, you can use shorter commands:

```bash
# Instead of specifying URLs every time:
pdi schema -c "postgresql://user:pass@cloud:5432/db" -e "postgresql://user:pass@edge:5432/db"

# You can simply run:
pdi schema

# Or override specific values:
pdi schema -s "custom_schema"
pdi schema -c "postgresql://different:url@host:5432/db"
```

### Setup Instructions

1. Copy the example file:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` with your actual database credentials:
   ```bash
   nano .env  # or your preferred editor
   ```

3. Now you can use PDI with minimal parameters:
   ```bash
   pdi schema          # Uses .env values
   pdi records         # Uses .env values
   pdi health -u $CLOUD_DB_URL  # Test cloud connection
   ```

### Advanced Environment Variable Usage

You can also use environment variables in combination with other options:

```bash
# Use .env for connections, but override schema
pdi schema -s "production_schema"

# Use .env for cloud, but specify different edge
pdi schema -e "postgresql://different:edge@host:5432/db"

# Mix environment variables with dump files
pdi schema --edge-dump backup.sql  # Uses CLOUD_DB_URL from .env

# Use custom output directory from .env
pdi schema --execute  # Will create files in OUTPUT_DIR from .env
```

## Error Handling

The tool provides detailed error messages for:
- Connection failures
- Authentication issues
- Schema access problems
- SQL execution errors

## License

MIT 