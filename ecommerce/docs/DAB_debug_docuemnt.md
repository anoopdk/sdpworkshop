# DAB Pipeline Debug Document

## 1. Purpose
This document captures the full debugging procedure used to identify the root cause(s) of recent Databricks pipeline failures in the ecommerce bundle deployment.

Scope includes:
- catalog/permission failure analysis
- variable substitution validation
- active-vs-legacy pipeline isolation
- runtime Python/import error triage
- corrective actions and prevention controls

Date of investigation: 2026-04-12 to 2026-04-13

---

## 2. Initial Symptom Report
Observed production-facing error pattern:
- `MISSING_CREATE_SCHEMA_PRIVILEGE` against catalog `ecommerce`

Initial user suspicion:
- variable substitution failure (`ecommerce_${var.catalog_env}` not resolving to `ecommerce_dev`)

---

## 3. Working Assumptions and Hypotheses
At start, multiple plausible causes existed:

H1. Bundle variable substitution is broken in current deployment.
H2. Correct bundle pipeline exists, but a different (legacy) pipeline is being executed.
H3. Catalog resolution is correct, but pipeline fails earlier due to Python/runtime module issues.
H4. Multiple historical failures are being conflated (old unresolved-variable errors + new runtime errors).

Debug strategy:
- Validate config source of truth first.
- Validate deployed pipeline spec second.
- Validate pipeline identity being run third.
- Validate runtime stacktrace separately from permission error.

---

## 4. Environment and Artefacts Used
Repository artefacts:
- `databricks.yml`
- `resources/pipelines/sdp-ecommerce_e2e.yml`
- `pipeline_errors.txt`
- `all_pipelines.json`

Databricks resources inspected:
- Pipeline `dev_anoopdk_sdp-ecommerce_e2e_dev` (ID: `6184e312-5926-41ef-b0da-92b995d3edb1`)
- Pipeline `sdp-ecommerce_e2e` (ID: `864ad47e-4225-495f-8187-eb8ea402e7b0`)
- Pipeline `sdp-ecommerce_e2e_prod` (ID: `872e65f1-be60-4220-aed5-d0b69561dde2`)

---

## 5. Step-by-Step Debug Procedure

### Step 1: Validate bundle config for env substitution intent
Files checked:
- `databricks.yml`
- `resources/pipelines/sdp-ecommerce_e2e.yml`

What was verified:
- target `dev` defines `variables.catalog_env: dev`
- pipeline declares `catalog: ecommerce_${var.catalog_env}`
- pipeline configuration injects `catalog_env: ${var.catalog_env}`

Inference:
- Desired behavior is explicit and correctly declared in IaC.

---

### Step 2: Validate effective deployment state (not just source files)
Commands run:
```powershell
databricks bundle validate -t dev --profile DEV
databricks bundle plan -t dev --profile DEV
databricks bundle summary -t dev --profile DEV
```

Key outcomes:
- validate: success
- plan: `0 to add, 0 to change, 0 to delete`
- summary identified active bundle pipeline resource and ID

Inference:
- Bundle deployment state is coherent; no pending drift from local source to deployed target at that point.

---

### Step 3: Inspect deployed pipeline spec to prove resolved catalog
Command run:
```powershell
databricks pipelines get 6184e312-5926-41ef-b0da-92b995d3edb1 --profile DEV
```

Critical evidence:
- `spec.catalog = ecommerce_dev`
- `spec.configuration.catalog_env = dev`
- `spec.deployment.kind = BUNDLE`

Inference:
- For the active bundle-managed dev pipeline, substitution is resolved correctly.
- This falsifies H1 for the active pipeline.

---

### Step 4: Enumerate all pipelines to detect identity confusion
Evidence source:
- `all_pipelines.json`

What was found:
- multiple similarly named pipelines coexist in workspace
- legacy `sdp-ecommerce_e2e` is still present

Inference:
- A user can accidentally run the wrong pipeline from UI, causing mixed failure signatures.

---

### Step 5: Compare bundle pipeline vs legacy pipeline specs
Command run:
```powershell
databricks pipelines get 6184e312-5926-41ef-b0da-92b995d3edb1 --profile DEV
databricks pipelines get 864ad47e-4225-495f-8187-eb8ea402e7b0 --profile DEV
```

Direct comparison result:
- bundle pipeline (`6184...`): `spec.catalog = ecommerce_dev`
- legacy pipeline (`864a...`): `spec.catalog = ecommerce`

Inference:
- Privilege failures against `ecommerce` originate from running legacy pipeline configuration, not current bundle target.
- H2 strongly supported.

---

### Step 6: Inspect event logs for actual runtime failure class on active bundle pipeline
Evidence source:
- `pipeline_errors.txt`

Observed active pipeline errors (`6184...`):
1. `ModuleNotFoundError: No module named 'config'` from bronze notebook import path.
2. `NameError: name 'sdp' is not defined` in helper expectation decorator execution context.
3. historical runs also showed `NameError: __file__ is not defined` in pipeline notebook runtime context.

Inference:
- Active bundle pipeline was failing due to runtime import/execution context issues after catalog resolution succeeded.
- H3 and H4 supported.

---

### Step 7: Distinguish historical unresolved-variable failures from current state
From historical events:
- `NO_SUCH_CATALOG_EXCEPTION: Catalog 'ecommerce_${var.catalog_env}' was not found`

Context:
- these errors belonged to older update runs and/or differently named pipeline instances
- current deployed bundle spec now resolves to `ecommerce_dev`

Inference:
- unresolved placeholder errors were historical artifacts, not current deployment truth.

---

## 6. Root Cause Statement
Primary root cause (permission error reported by user):
- Wrong pipeline executed (legacy pipeline ID `864ad47e-4225-495f-8187-eb8ea402e7b0`) with hard-coded `catalog = ecommerce`.
- User principal lacked create-schema privilege on `ecommerce`, producing privilege errors.

Secondary root causes (active pipeline failures):
- Runtime code/import issues in notebooks and helper path/decorator context, independent of catalog substitution.

Therefore:
- Variable substitution was not the active root cause for the bundle-managed dev pipeline.
- Pipeline identity mismatch was the root cause for the specific catalog privilege symptom.

---

## 7. Corrective Actions Applied During Session
Code-level corrections applied:

1. Bronze orchestration import hardening
- removed dependency on missing `config.common.tags.bronze_tags`
- introduced local `BRONZE_TAGS` mapping

2. Metadata index naming consistency
- orders keep `_order_index`
- customers use `_cust_index`
- products use `_prod_index`
- silver dedup and `sequence_by` updated accordingly

3. Documentation consistency updates
- bronze/silver/end-to-end docs aligned to new metadata index contract

Operational correction:
- confirmed correct pipeline to run is `dev_anoopdk_sdp-ecommerce_e2e_dev` (ID `6184...`)
- identified legacy pipeline as unsafe for current dev workflow unless intentionally maintained

---

## 8. Verification Checklist (Runbook)
Use this checklist every time a similar issue appears:

1. Confirm target config
```powershell
rg "catalog_env|catalog:" databricks.yml resources/pipelines -n
```

2. Validate bundle state
```powershell
databricks bundle validate -t dev --profile DEV
databricks bundle plan -t dev --profile DEV
databricks bundle summary -t dev --profile DEV
```

3. Confirm deployed spec for active pipeline
```powershell
databricks pipelines get <ACTIVE_PIPELINE_ID> --profile DEV
```
Check:
- `spec.catalog`
- `spec.configuration.catalog_env`
- `spec.deployment.kind`

4. Enumerate workspace pipelines (avoid wrong-run mistakes)
```powershell
databricks pipelines list-pipelines --profile DEV
```

5. Pull latest update events for failing run
```powershell
databricks pipelines list-pipeline-events <PIPELINE_ID> --profile DEV
```

6. Classify failure type before fixing
- permission/catalog mismatch
- unresolved placeholder
- Python import/runtime context
- data contract/schema issue

---

## 9. Prevention Controls
Recommended controls to avoid recurrence:

1. Pipeline hygiene
- archive or delete stale legacy pipelines not part of bundle lifecycle
- use a strict naming convention with environment prefixes only from bundle

2. Release gate
- add pre-run check script that compares expected catalog (`ecommerce_dev`) vs deployed `spec.catalog`

3. Operational SOP
- run only pipeline IDs published by `bundle summary`
- avoid manually triggering similarly named old pipelines from UI

4. Runtime robustness
- avoid fragile notebook imports that assume `__file__` exists
- keep helper modules self-contained with explicit dependencies

---

## 10. Final Conclusion
The investigation proved that:
- The user-visible catalog privilege error was caused by executing a legacy pipeline configured with `catalog = ecommerce`.
- The active bundle pipeline correctly resolved to `catalog = ecommerce_dev`.
- Independent runtime Python/import issues also existed and caused separate failures in recent updates.

This was a multi-cause incident, but the specific catalog permission symptom was pipeline identity mismatch, not current variable substitution failure.
