---
title: "Eval"
order: 6
---

# Eval

`zipline hub eval` is the fastest way to validate a Chronon config while you are authoring it. It compiles the conf, syncs the compiled lineage to Hub, and asks the eval service to validate the final conf you named against the full upstream graph.

Note: The easiest way to call Eval is through the Zipline VSCode / Cursor extension.

You can call `eval` on `Join`s, `GroupBy`s or `StagingQuery`s.

You do **not** need to run `eval` on each node in a lineage. It happens automatically for you.

For example, for `v1 = Join(...)` defined [here](https://github.com/zipline-ai/chronon/blob/main/python/test/canary/joins/gcp/demo.py):

```bash
zipline hub eval compiled/joins/gcp/demo.v1__1
```
(or just click the button in VSCode)

will automatically validate:

1. the final `Join`
2. the left-side `StagingQuery`
3. each `JoinPart` `GroupBy`
4. each upstream `StagingQuery` behind those `GroupBy`s

Those upstream confs do not need to have been backfilled, deployed, or previously materialized by Chronon. `eval` resolves them from the compiled lineage it just synced. The things that still need to exist are the external tables that those upstream confs read from.

## What `eval` checks

- source tables exist and are accessible
- projected columns and types match your config
- `StagingQuery` SQL parses and its output schema is valid
- `GroupBy` selects, aggregations, and derivations type-check
- `Join` key mappings, left-side expressions, and derivations resolve correctly
- the full dependency graph can be resolved from the final conf you named

If `EVAL_URL` is configured in your team's `env.common` in `teams.py`, the CLI uses it automatically. You can override it with `--eval-url` when needed.

## Reading the default output

Here is a shortened excerpt from a real eval run on `compiled/joins/gcp/demo.v1__1` in the canary repo:

```text
Join Configuration: gcp.demo.v1__1
  - Left table: data.gcp_exports_user_activities__0
  - Join parts: 3
  - Conf dependencies: 4
  - External tables: 0
  - Output Schema:
   [left]    ts: long
   [left]    row_id: string
   [left]    user_id: string
   [left]    listing_id: long
   [left]    ds: string
   [joinPart: gcp.user_activities.v1__1]    user_id_view_event_sum_1d: long
   [joinPart: gcp.dim_listings.v1__0]       listing_id_headline: string
   [joinPart: gcp.dim_merchants.v1__0]      merchant__listing_id_primary_category: string

Lineage:
[Join] gcp.demo.v1__1
├── ✅ [StagingQuery] gcp.exports.user_activities__0
├── ✅ [GroupBy] gcp.user_activities.v1__1
│   └── ✅ [StagingQuery] gcp.exports.user_activities__0
├── ✅ [GroupBy] gcp.dim_listings.v1__0
│   └── ✅ [StagingQuery] gcp.exports.dim_listings__0
└── ✅ [GroupBy] gcp.dim_merchants.v1__0
    └── ✅ [StagingQuery] gcp.exports.dim_merchants__0
```

If there is a semantic error anywhere in the graph, i.e. referencing an invalid column name, it would show up here as well.

1. Author or edit the final conf.
2. Run `zipline compile`.
3. Run `zipline hub eval` on that final conf.
4. Use `--format=json` when you want the full upstream schema tree.
5. Use `--generate-test-config` and `--test-data-path` when you want concrete computed rows before backfill.

For local eval service setup and CI-oriented usage, see [Testing `GroupBy`s, `Join`s and `StagingQuery`s](/docs/running_on_zipline_hub/Test).
