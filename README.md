# Ecommerce Spark Declarative Pipelines

**Delivered by:** [Zynx Technologies Ltd](https://www.zynxtechnologies.com/)  
**Portfolio host (pre-fork):** `anoopdk/sdpworkshop`  
**Domain client (sanitised):** Zynx x Retail / Ecommerce Client  
**Engagement type:** Implementation Delivery  
**Portfolio version:** `1.0.0-portfolio`

> Inspection note: Client identity is domain-aliased for public portfolio use. This repository is structured for authenticity review of delivery artefacts, architecture, and engagement scope.

## Project overview

The Retail / Ecommerce Client required an end-to-end Databricks pipeline for ecommerce entities with centralised data-quality expectations, using Spark Declarative Pipelines and Asset Bundles for repeatable deployment.

### Solution delivered

Zynx implemented a Bronze -> Silver -> Gold ecommerce SDP solution under `ecommerce/`, including config-driven expectations, jobs/pipelines YAML, fixtures, design docs, and test hooks.

## Engagement levels

| # | Level |
| --- | --- |
| 1 | Discovery & Source Profiling |
| 2 | Medallion Design |
| 3 | SDP Implementation |
| 4 | DQ Expectations & Handover |

| Field | Value |
| --- | --- |
| Engagement posture | Delivery portfolio artefact prepared for audit / inspection |
| Confidentiality | Domain alias only; no real client legal name in public docs |
| Primary artefacts | Code/notebooks/docs already in-repo + Zynx standard pack under `docs/` and `decks/` |

## Architecture

```mermaid
flowchart LR
  RAW[Raw files / volume] --> BR[Bronze]
  BR --> SV[Silver + expectations]
  SV --> GD[Gold aggregations]
  CFG[Central expectation dictionaries] --> SV
  DAB[DAB jobs + pipelines] --> BR
  DAB --> SV
  DAB --> GD
```

### Implementation flow

```mermaid
flowchart TB
  FIX[Fixtures + UC setup] --> BRN[Bronze notebooks/config]
  BRN --> SIL[Silver transformations]
  SIL --> GOL[Gold notebooks]
  JOB[job_ecommerce_e2e] --> BRN
  JOB --> SIL
  JOB --> GOL
```

## Technology stack

`Databricks Asset Bundles`, `Spark Declarative Pipelines`, `Unity Catalog`, `Python expectations`, `Medallion`

## Repository map

| Path | Purpose |
| --- | --- |
| `README.md` | Project overview + engagement levels |
| `docs/` | Problem, solution, architecture, implementation, runbook, audit factsheet |
| `decks/` | 3-slide delivery overview (Markdown + HTML) |
| `src/` / project subtree | Implementation assets (or placeholders where design-led) |
| `tests/` | Tests or placeholders |
| `scripts/` | Operational / helper scripts |
| `configs/` | Non-secret configuration templates |
| `.env.example` | Environment variable placeholders |

### Key existing delivery paths

- `ecommerce/docs/`
- `ecommerce/src/`
- `ecommerce/resources/`
- `ecommerce/databricks.yml`

## How to inspect this repository

1. Read this README (overview + engagement).
2. Open `docs/audit/PROJECT-FACTSHEET.md`.
3. Review architecture in `docs/03-architecture.md`.
4. Walk implementation evidence listed in the factsheet.
5. Open `decks/overview.html` for the 3-slide executive summary.

## Related Zynx context

Zynx Technologies Ltd provides IT consulting across cloud, data analytics, and digital delivery. This repository is part of the public delivery portfolio hosted on `anoopdk` prior to organisation-level fork.

## License

See `LICENSE`.
