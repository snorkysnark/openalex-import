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
uv run download-examples-each.py
```

## 2. Convert to CSV


*This script assumes your downloaded snapshot is in openalex-snapshot and you've made a directory csv-files to hold the CSV files.*

*Edit SNAPSHOT_DIR and CSV_DIR at the top of the script to read or write the files somewhere else.*


```
python flatten-openalex-jsonl.py
```

## 3. Upload to relational database

Postgres:

```
psql -d openalex -f postgres/openalex-pg-schema.sql
psql -d openalex -f postgres/copy-openalex-csv.sql
```

Duckdb:

```
duckdb openalex-shapshot.duckdb -f duckdb/openalex-duckdb-schema.sql
duckdb openalex-shapshot.duckdb -f duckdb/copy-openalex-csv.sql
```
