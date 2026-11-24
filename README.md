# Olist DW on Hadoop

Kimball star schema built from the Olist Brazilian E-Commerce dataset (Kaggle),
loaded in batch and streaming mode on a Hortonworks HDP 2.5 sandbox managed by
Ambari (HDFS, YARN, Hive 1.2, Spark2 2.0, Oozie).

## Pipeline

```
MAC                                        SANDBOX (HDP 2.5)
extract_kaggle.sh  -> ~/olist-landing/<date>/   (Kaggle API + MANIFEST.tsv)
transfer_to_sandbox.sh -> /opt/olist/landing/<date>/
                                           ingest_olist.sh
                                             validate (files, md5, line counts)
                                             -> HDFS /data/olist/raw/<table>/load_date=<date>/
                                             -> HDFS /data/olist/raw/_ingest/load_date=<date>/_SUCCESS
                                             -> audit log /opt/olist/logs/ingest_audit.log
                                           Spark jobs on YARN (profile, dims, facts)
                                           Hive external tables over /data/olist/curated
                                           Oozie workflow + coordinator (orchestration)
```

## Repository layout

| Path | Purpose |
|---|---|
| `conf/olist.conf` | All paths and settings in one place (no secrets) |
| `ingest/extract_kaggle.sh` | Mac: download from the Kaggle API, write manifest |
| `ingest/transfer_to_sandbox.sh` | Mac: copy a landing folder to the sandbox |
| `ingest/ingest_olist.sh` | Sandbox: validate, load to HDFS raw, write `_SUCCESS`, audit |
| `spark/common.py` | Shared readers (empty field -> null, multi-line reviews), keys, Unknown rows |
| `spark/profile_olist.py` | Data profiling / grain checks for one load date |
| `spark/build_dims.py` | Builds the 7 dimension tables as Parquet in `/data/olist/curated` |
| `spark/build_facts.py` | Builds the 4 fact tables (surrogate key lookups, reconciliation checks) |
| `run_spark.sh` | Sandbox: submit a Spark job to YARN with sandbox-sized memory |
| `run_hive.sh` | Sandbox: run a Hive SQL file via beeline; database and HDFS path passed in from the conf |
| `hive/ddl_dims.sql` | Hive external tables over the dimension Parquet files |
| `hive/ddl_facts.sql` | Hive external tables over the fact Parquet files (partitioned by purchase year) |
| `hive/queries.sql` | Step 6: referential-integrity check and business queries per process, plus drill-across |
| `oozie/workflow.xml` | Oozie batch workflow: raw-load check -> dims -> facts -> Hive DDL -> queries -> batch `_SUCCESS` |
| `oozie/coordinator.xml` | Oozie daily coordinator: starts the workflow when a day's raw `_SUCCESS` flag appears |
| `oozie/setup_ssh.sh` | One-time: lets the Oozie server run the pipeline scripts over SSH |
| `run_oozie.sh` | Sandbox: builds `job.properties` from the conf and submits / checks Oozie jobs |
| `deploy.sh` | Mac: ship the repo to the sandbox and HDFS |
| `run_pipeline.sh` | Mac: one command - extract, transfer, ingest |

## One-time setup (Mac)

1. Kaggle API token saved in `~/.kaggle/access_token` (never committed).
2. SSH key login to the sandbox, alias `olist-sandbox` in `~/.ssh/config`.

## Usage

```bash
./deploy.sh                 # after every code change (git commit first)
./run_pipeline.sh           # extract + transfer + ingest for today
./run_pipeline.sh 2026-10-27  # or for a specific load date

# on the sandbox (or via ssh olist-sandbox "...")
/opt/olist/app/run_oozie.sh run 2026-11-03                  # whole batch for one load date
/opt/olist/app/run_oozie.sh schedule 2026-11-03 2026-11-10  # daily, data-triggered
/opt/olist/app/run_oozie.sh status <job-id>
```

## Data rules

Decisions taken where the source data does not settle the answer. Each is
applied in the Spark job named.

| Rule | Why | Effect | Where |
|---|---|---|---|
| A fact reference with no matching dimension row gets key **-1** (the Unknown member) | Every fact row must join to every dimension; null keys drop rows from joins | e.g. 278 orders whose customer zip is not in the geolocation file | `build_facts.py` |
| A milestone that has not happened yet gets date key **-1** ("Unknown / not yet occurred") | Accumulating snapshot rows exist before all milestones are reached | 2,965 undelivered orders have `delivered_date_key = -1` | `build_facts.py` |
| Impossible date sequences are **flagged, not fixed** (`date_anomaly_flag = 1`) | The warehouse must not invent data; users decide whether to exclude them | 1,382 orders flagged | `build_facts.py` |
| **fact_review keeps the latest review per order** (by creation date, then answer time, then review_id) | Gives a clean, unique grain (`order_id`) that links to the order facts | 99,224 source reviews -> 98,673 rows; earlier reviews of 547 orders stay in the raw zone | `build_facts.py` |
| Orders with no items are **kept** in fact_order_fulfillment with `item_count = 0`, `order_value = 0` | No order may be lost from the fulfillment process | 775 orders | `build_facts.py` |
| The **date dimension's calendar** runs from 1 January of the earliest year to 31 December of the latest year found in any date column of the load | The calendar must cover every date a fact can reference; it grows automatically with new data | 2016-01-01 to 2020-12-31 for load 2026-11-03 | `build_dims.py` |
| Customer location changes are kept as **SCD Type 2** versions; facts point to the version valid at purchase time | Sales are reported where the customer lived when they bought | 252 people with more than one version | `build_dims.py` |
| Blank product category becomes **'unknown'**; a category with no English translation keeps its **Portuguese** name | Every product needs a category to group by | 610 unknown, 2 untranslated | `build_dims.py` |
| Misspelt source categories are **corrected** (`costruction` -> `construction`, `fashio` -> `fashion`, `confort` -> `comfort`) | Consistent, readable labels | 4 categories | `build_dims.py` |
| Each zip prefix takes its **most frequent** city/state and the **mean** coordinates inside Brazil's bounding box | The geolocation file has many rows per zip, some outside Brazil | 19,016 zip prefixes | `build_dims.py` |

## Engineering conventions

- Raw zone is immutable; each delivery is a full snapshot under its own `load_date=` partition.
- Nothing environment-specific is written in code or SQL: paths, database, memory and source tables live in `conf/olist.conf`; SQL files receive them as Hive variables (`${hivevar:DB}`, `${hivevar:CURATED}`); reported figures are computed at run time.
- Every step is idempotent: re-running a load date replaces that date's output.
- Files are validated (presence, md5 checksum, minimum line count) before they reach HDFS.
- A `_SUCCESS` flag marks a complete load; downstream jobs only start when it exists. A finished batch writes its own flag in `curated/_batch/load_date=<date>/`.
- Spark jobs end with a quality gate (duplicate keys, reconciliation to raw); a failure exits non-zero and stops the Oozie workflow.
- Sandbox exceptions: single node, `root` user, permissive HDFS permissions, no Kerberos/Ranger.
