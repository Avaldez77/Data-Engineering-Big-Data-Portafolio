# Data Engineering & Big Data Portfolio

This portfolio collects practical work in SQL analytics, data modeling,
pipeline validation, event processing, and distributed-data concepts. The
examples are learning artifacts and case studies; they do not claim measured
production throughput or access to company systems.

## Connected portfolio

This repository covers the data platform and pipeline layer of a four-part
portfolio. See the [profile overview](https://github.com/Avaldez77/Avaldez77)
or explore [Data Analytics](https://github.com/Avaldez77/Data-Analytics-Portafolio),
[Data Science](https://github.com/Avaldez77/Data-science-Portafolio), and
[RPA & Process Automation](https://github.com/Avaldez77/RPAs-Portafolio).

## Featured end-to-end case

[Capital Bikeshare Demand Intelligence](https://github.com/Avaldez77/BikeShare-Demand-Intelligence)
uses an attributed public dataset to build a DuckDB warehouse, quality gate,
daily KPIs, forecast evaluation, and a dry-run report package.

## Areas

| Area | Topics |
| --- | --- |
| [Data Engineering](./Data%20Engineering/Data%20Engineering.md) | Relational and nested SQL, dimensional modeling, operational analytics, end-to-end analysis |
| [Big Data](./Big%20Data/Big%20Data.md) | Market-basket analysis, operational logs, retail analytics, urban equity |
| [Hands-on exercises](./Exercises/README.md) | SQLite Bronze/Silver/Gold quality pipeline and DuckDB analytical warehouse, with quarantine and tests |

## Start with the runnable exercise

The [Daily Event Quality exercise](./Exercises/Daily%20Event%20Quality/README.md)
uses only Python's standard library and SQLite. From its directory:

```bash
python run_demo.py
python -m unittest discover -s tests -v
```

The [DuckDB warehouse exercise](./Exercises/DuckDB%20Warehouse/README.md)
uses a pinned DuckDB Python package and a local database; it does not require
cloud access or external services.

## Working principles

- Separate raw, validated, and analytical data layers.
- State the grain, schema, assumptions, and business rules of each output.
- Test duplicates, malformed values, late arrivals, and boundary conditions.
- Keep credentials and production connectivity out of public examples.
- Use synthetic data for reproducible demos and label illustrative results.
