# Data Engineering & Big Data Portfolio

This portfolio collects practical work in SQL analytics, data modeling,
pipeline validation, event processing, and distributed-data concepts. The
examples are learning artifacts and case studies; they do not claim measured
production throughput or access to company systems.

## Areas

| Area | Topics |
| --- | --- |
| [Data Engineering](./Data%20Engineering/Data%20Engineering.md) | Relational and nested SQL, dimensional modeling, operational analytics, end-to-end analysis |
| [Big Data](./Big%20Data/Big%20Data.md) | Market-basket analysis, operational logs, retail analytics, urban equity |
| [Hands-on exercises](./Exercises/README.md) | Runnable Bronze/Silver/Gold event pipeline with quarantine, tests, and extension challenges |

## Start with the runnable exercise

The [Daily Event Quality exercise](./Exercises/Daily%20Event%20Quality/README.md)
uses only Python's standard library and SQLite. From its directory:

```bash
python run_demo.py
python -m unittest discover -s tests -v
```

## Working principles

- Separate raw, validated, and analytical data layers.
- State the grain, schema, assumptions, and business rules of each output.
- Test duplicates, malformed values, late arrivals, and boundary conditions.
- Keep credentials and production connectivity out of public examples.
- Use synthetic data for reproducible demos and label illustrative results.
