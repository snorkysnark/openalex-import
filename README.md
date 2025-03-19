# OpenAlex to SQL example

Based on https://docs.openalex.org/download-all-data/upload-to-your-database/load-to-a-relational-database

## Step 1: Downloading

The data must have the following directory structure, where `*` is an arbitrary string

```
└── openalex-shapshot/data
    ├── authors
    │   └── *
    │       ├── *.gz
    │       ├── ...
    │       └── *.gz
    ├── funders
    ├── institutions
    ├── publishers
    ├── sources
    ├── topics
    └── works
```

You can either download the full 300GB+ shapshot

```
aws s3 sync "s3://openalex" "openalex-snapshot" --no-sign-request
```

or individual entities from the API. The following script downloads 5 random entities of each type

```
uv run python download-examples-each.py openalex-snapshot
```

## Convert to CSV (optional)


*This script assumes your downloaded snapshot is in openalex-snapshot and you've made a directory csv-files to hold the CSV files.*

*Edit SNAPSHOT_DIR and CSV_DIR at the top of the script to read or write the files somewhere else.*


```
python flatten-openalex-jsonl.py
```

## Import directly to database

First of all, you must create the schema:

```
psql -d openalex -f postgres/openalex-pg-schema.sql

duckdb openalex-shapshot.duckdb -f duckdb/openalex-duckdb-schema.sql
```

Write snapshot directly to database:

```
uv run python db-import.py openalex-snapshot postgresql:///openalex

uv run python db-import.py openalex-snapshot duckdb:///openalex-shapshot.duckdb
```
