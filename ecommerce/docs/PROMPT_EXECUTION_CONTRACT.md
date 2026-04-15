# Prompt Execution Contract

Use this template when asking implementation tasks so requirements are explicit, testable, and verifiable.

## 1) Objective
State the exact end result in one sentence.

Example:
- Fix the Silver Auto CDC flow so all stage sources are streaming temporary views and the pipeline update completes without analysis errors.

## 2) Scope
List files that may be edited.

Example:
- src/notebook/bronze/bronze_orchestration.py
- src/notebook/silver/silver_transformation.py
- src/notebook/gold/gold_aggregation.py

List files that must not be edited.

Example:
- fixtures/**
- resources/jobs/**

## 3) Hard Technical Constraints
State architecture rules that are non-negotiable.

Example:
- Auto CDC sources must be streaming temporary views.
- Do not use materialized views as Auto CDC preprocessing sources.
- Keep Unity Catalog naming pattern: ecommerce_<env>.<schema>.<table>.
- Preserve SCD2 behavior and delete handling.

## 4) Data Contract Rules
State expected columns and semantics.

Example:
- Bronze must include: _ingest_ts, _file_mod_time, _source_file.
- If _metadata.row_index is unavailable, use deterministic file/time tie-break fields.
- Silver stage output must include all sequence_by fields used by create_auto_cdc_flow.

## 5) Definition of Done (DoD)
Provide binary pass/fail checks.

Example:
- No duplicate table definitions in pipeline files.
- No unresolved dataset errors in update analysis.
- No FIELD_NOT_FOUND on _metadata.row_index.
- Databricks bundle validation passes.
- Pipeline update reaches COMPLETED.

## 6) Validation Required (Mandatory)
Require execution and evidence.

Example:
- Run: databricks bundle validate -t dev --profile DEV
- Run: databricks bundle deploy -t dev --profile DEV
- Run: databricks pipelines start-update <PIPELINE_ID> --profile DEV
- Report: final update state and any remaining error messages

## 7) Output Format Required
Force a predictable response structure.

Example:
1. Plan (short)
2. Files changed
3. Exact fixes made
4. Validation command summary
5. Remaining risks (if any)

## 8) Ambiguity Policy
Require clarification before coding when assumptions are needed.

Use this line in your prompt:
- If any requirement is ambiguous, stop and ask before editing.

## 9) Change Control Policy
Keep edits minimal and targeted.

Use this line in your prompt:
- Do not make unrelated refactors or formatting-only changes.

## 10) Reusable Prompt (General)
Copy/paste and fill this block:

I want you to implement the following task.

Objective:
- <what success looks like>

Scope (allowed files):
- <file1>
- <file2>

Out of scope (do not edit):
- <file3>

Hard constraints:
- <constraint 1>
- <constraint 2>

Definition of done:
- <testable check 1>
- <testable check 2>

Validation required:
- <command 1>
- <command 2>

Output format:
- Plan
- Changes
- Validation evidence
- Risks

Ambiguity rule:
- If anything is ambiguous, ask first.

## 11) Databricks-Specific Prompt (This Project)
Copy/paste and fill this block:

Implement this Databricks pipeline change.

Objective:
- <your target behavior>

Scope (allowed):
- src/notebook/bronze/bronze_orchestration.py
- src/notebook/silver/silver_transformation.py
- src/notebook/gold/gold_aggregation.py
- documents/*.md

Hard constraints:
- Auto CDC preprocessing sources must be sdp.temporary_view with streaming DataFrame logic.
- Auto CDC sequence_by fields must exist in stage output.
- Avoid _metadata.row_index dependency.
- Preserve SCD2 semantics and delete handling.

Definition of done:
- bundle validate passes for dev target.
- pipeline update has no unresolved dataset errors.
- pipeline update reaches COMPLETED.

Validation required:
- databricks bundle validate -t dev --profile DEV
- databricks bundle deploy -t dev --profile DEV
- databricks pipelines start-update <PIPELINE_ID> --profile DEV
- databricks pipelines get-update <PIPELINE_ID> <UPDATE_ID> --profile DEV

Output format:
- What changed
- Why it changed
- Validation results
- Open issues (if any)
