# cosmarket-prod-public

Public demo target for the Astro Investigation Agent auto-fix PR flow.

This repo contains a single intentionally-broken Airflow DAG (`fetch_cargo_manifests`) and its supporting SQL DDL. It exists so an automated workflow — running elsewhere in a private deployment — can:

1. Detect the failure of `fetch_cargo_manifests` in production.
2. Call the Astro Investigation Agent for a structured root-cause diagnosis.
3. Send the diagnosis plus the source file to an LLM to produce a fix.
4. Open a PR against this repo with the proposed fix.

## The bug

`dags/examples/fetch_cargo_manifests.py` ingests synthetic interplanetary cargo manifests, stages them to S3 as Parquet, and copies them into Snowflake. One column — `cargo_fill_pct` — is generated as a formatted string with a `%` suffix (e.g. `"73.4%"`) instead of a numeric value. Snowflake's `COPY INTO` rejects the cast to `NUMBER(5,2)`:

```
Failed to cast variant value "73.4%" to FIXED
```

## Layout

- `dags/examples/fetch_cargo_manifests.py` — the failing DAG
- `include/sql/create_tables/logistics/cargo_manifests.sql` — table DDL the DAG creates on first run

## What this repo is *not*

- Not a runnable Astro project on its own — there's no `Dockerfile`, `requirements.txt`, or supporting infrastructure here. The DAG runs in a separate (private) deployment.
- Not the home of the auto-fix automation — that lives in the same private deployment as the failing DAG.
