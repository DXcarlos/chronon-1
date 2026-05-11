---
title: "Eval"
order: 6
---

# Eval

`zipline hub eval` is the fastest way to validate a Chronon config while you are authoring it. It compiles the conf, syncs the compiled lineage to Hub, and asks the eval service to validate the final conf you named against the full upstream graph.

## The mental model

You only call `eval` on the final conf you care about:

- a `Join` when you are validating the full feature surface for training or serving
- a `GroupBy` when you are validating a reusable feature source
- a `StagingQuery` when you are validating a raw export or intermediate dataset

You do **not** need to run `eval` on each upstream `StagingQuery` or `GroupBy` first.

For example, the `v1 = Join(...)` defined in `python/test/canary/joins/gcp/demo.py` compiles to `compiled/joins/gcp/demo.v1__1`. Running:

```bash
zipline hub eval compiled/joins/gcp/demo.v1__1
```

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

The important labels are:

- `[left]`: columns coming from the left side of a `Join`
- `[joinPart: ...]`: columns contributed by a specific `GroupBy`
- `[derivation]`: columns created by `Join.derivations`
- `[key]`: key columns in a `GroupBy`
- `[aggregation]`: aggregated output columns in a `GroupBy`
- `[no agg]`: pass-through columns in an entity-style `GroupBy`

The summary lines mean:

- `Left table`: the materialized table or view that the join evaluates on its left side
- `Join parts`: how many `JoinPart`s the final join includes
- `Conf dependencies`: the direct upstream Chronon confs the final conf depends on
- `External tables`: raw warehouse tables referenced directly by the conf being summarized

![Eval command demonstration](../../images/eval_sample.gif)

## Getting the full upstream schema

The default human-readable output is optimized for quick authoring feedback. If you want the schema for every upstream node in a machine-readable form, run:

```bash
zipline hub eval compiled/joins/gcp/demo.v1__1 --format=json
```

In that mode, the response includes the top-level summary plus a recursive `upstreamResponse` tree. Here is a shortened excerpt from the same canary eval:

```json
{
  "confName": "gcp.demo.v1__1",
  "success": 1,
  "shortMessage": "✅ Join: gcp.demo.v1__1 (4 deps, 0 external)",
  "upstreamResponse": [
    {
      "confName": "gcp.exports.user_activities__0",
      "message": "StagingQuery Configuration: gcp.exports.user_activities__0 ...",
      "schemaInfo": {
        "external_table_0": "demo.`user-activities`"
      }
    },
    {
      "confName": "gcp.user_activities.v1__1",
      "message": "GroupBy Configuration: gcp.user_activities.v1__1 ..."
    }
  ]
}
```

This is the easiest way to inspect the full lineage schema for the final conf you are evaluating:

- the top-level `message` describes the final `Join`, `GroupBy`, or `StagingQuery`
- each `upstreamResponse` entry describes one direct dependency
- each dependency can have its own nested `upstreamResponse`
- `schemaInfo` calls out external tables when a node reads directly from the warehouse
- `depth` and `confType` are bookkeeping fields for the recursive response and are usually safe to ignore

For the `gcp.demo.v1__1` example, the JSON response includes:

- the `Join` output schema, including left-side columns and each `JoinPart`'s output columns
- the `gcp.user_activities.v1__1` `GroupBy` output schema, including `[key]` and `[aggregation]` columns
- the `gcp.dim_listings.v1__0` and `gcp.dim_merchants.v1__0` entity-style `GroupBy` output schemas, including `[no agg]` columns
- the output schema for each upstream `StagingQuery`, plus the raw external table name each one reads from

## Evaluating with sample rows

`eval` can also compute concrete outputs from fixture data instead of only checking schemas.

```bash
# 1. Generate a YAML skeleton that matches the raw input tables in the lineage
zipline hub eval compiled/joins/gcp/demo.v1__1 --generate-test-config

# 2. Fill in the generated YAML

# 3. Run eval against those rows
zipline hub eval compiled/joins/gcp/demo.v1__1 --test-data-path test-data.yaml
```

With `--test-data-path`, eval still returns the schema and lineage summary, then appends temporary catalog tables and computed output rows. In the canary `gcp.demo.v1__1` example, the response included a `data.gcp_demo_v1__1` table with 6 evaluated rows, which is useful for checking:

- point-in-time join behavior
- aggregation windows on concrete timestamps
- derivation logic on real sample values
- null handling and key matching

## Recommended workflow

1. Author or edit the final conf.
2. Run `zipline compile`.
3. Run `zipline hub eval` on that final conf.
4. Use `--format=json` when you want the full upstream schema tree.
5. Use `--generate-test-config` and `--test-data-path` when you want concrete computed rows before backfill.

For local eval service setup and CI-oriented usage, see [Testing `GroupBy`s, `Join`s and `StagingQuery`s](/docs/running_on_zipline_hub/Test).
