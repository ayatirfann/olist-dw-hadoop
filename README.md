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
| `spark/profile_olist.py` | Data profiling / grain checks for one load date |
| `hive/` | Hive DDL and validation queries (later) |
| `oozie/` | Oozie workflow and coordinator (later) |
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
```

## Engineering conventions

- Raw zone is immutable; each delivery is a full snapshot under its own `load_date=` partition.
- Every step is idempotent: re-running a load date replaces that date's output.
- Files are validated (presence, md5 checksum, minimum line count) before they reach HDFS.
- A `_SUCCESS` flag marks a complete load; downstream jobs only start when it exists.
- Sandbox exceptions: single node, `root` user, permissive HDFS permissions, no Kerberos/Ranger.
