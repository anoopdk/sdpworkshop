# Architecture

## Context diagram

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

## Implementation flow

```mermaid
flowchart TB
  FIX[Fixtures + UC setup] --> BRN[Bronze notebooks/config]
  BRN --> SIL[Silver transformations]
  SIL --> GOL[Gold notebooks]
  JOB[job_ecommerce_e2e] --> BRN
  JOB --> SIL
  JOB --> GOL
```

## Design notes

- Prefer layered medallion / governed platform patterns where applicable.
- Keep configuration and secrets outside Git.
- Preserve auditability: documentation must match repository evidence.
