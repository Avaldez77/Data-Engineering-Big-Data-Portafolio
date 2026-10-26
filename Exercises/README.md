# Hands-on exercises

The exercises use only synthetic data and local tools. No cloud credentials,
customer records, or company infrastructure are needed.

## Daily operational event quality

Start with [Daily Event Quality](./Daily%20Event%20Quality/README.md). It
provides a runnable SQLite-backed Bronze/Silver/Gold pipeline, expected
aggregates, tests, and follow-up tasks for quarantine, alerting, and idempotent
incremental loads.

## Suggested extensions for the existing case studies

- **Market Basket:** compare support and confidence thresholds, then explain
  how filtering rare items changes the candidate search space.
- **Operational Logs:** add a deterministic replay test and quantify duplicate
  and rejected-event rates before and after a parsing change.
- **Retail Analytics:** define a grain for each fact table and test referential
  integrity before computing a KPI.

For every extension, document the source schema, assumptions, expected output,
quality checks, and the command needed to reproduce the result.
