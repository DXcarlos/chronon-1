---
title: "Naming and Versioning"
order: 8
---


# Naming and Versioning

## Naming

`GroupBy`s, `StagingQuery`s and `Join`s are identified by four components:

1. **Team**: Files are organized inside a team directory. I.e. `group_bys/my_team/...`
2. **File path**: There may be subdirectories within the team directory, or files directly. I.e. `group_bys/my_team/user_features.py`
3. **Variable name**: Within the file, the entity is assigned to a Python variable. I.e. `purchase_features = GroupBy(...)`. See more details below.
4. **Version**: This is an argument to the constructor. I.e. `GroupBy(..., version=0)`. See more details below.

These four components combine to fully identify an entity. I.e. `my_team.user_features.purchase_features__0`.

This will be the name that is used for fetching features, and also corresponds to the output table name for backfilled data (`my_team_user_features_purchase_features__0`).


## Variable naming

Use the Python variable name to describe what the entity represents. Use the `version=` argument to track iterations of that entity over time.

Good variable names depend on the entity type:

- For a `GroupBy`, name the feature family. This is often related to the source being aggregated or passed through, such as `purchase_features` or `recent_listing_views`.
- For a `Join`, name the model, training set, or serving surface it powers, such as `fraud_training_set` or `ranking_search`.
- For a `StagingQuery`, name the semantics of the transformation it runs, such as `hydrated_interactions` or `checkouts_driver`.

If you have multiple entities in the same file, use the variable name to capture the meaningful difference between them. For example, say you have features defined on user activities, and you want one set that filters out bot traffic and one that does not.

Inside your `user_activities.py` file you might define:

```
no_bots = GroupBy(...) # Includes a filter clause on the source

with_bots = GroupBy(...) # No filter clause
```

## Versioning

`GroupBy`s, `StagingQuery`s and `Join`s all take a `version: int` argument in their constructor, i.e.:

```
purchase_features = GroupBy(
    ...
    version=0
)
```

When compiling, the version becomes part of the name of the entity as a `__{v}` suffix. In this example it would be `{team}.{file}.purchase_features__0`.

### Developer flow for iterating on versions

The version argument is the normal way to make an in-place change to an existing entity. For example, say you want to add some features to a `GroupBy`. A user would:

1. Make a git branch on their Chronon directory
2. Keep the variable name the same, bump `version=`, and add their new feature definitions
3. Test it using the Zipline/Chronon CLI (see docs on testing)
4. (Optionally) Deploy the branch with the new version for A/B testing (see docs on deploying)
5. Merge the branch to production

Keeping the entity name stable and bumping `version=` has a number of benefits:

1. Compute reuse: When running a backfill with your new `GroupBy` or `Join`, only the new features are computed. The unchanged ones are reused from existing backfills wherever possible.
2. Downstream consumers that reference that entity will automatically migrate to the new version when you merge to main. Schema incompatibilities caused by changing/removing features are caught at compile time.


## Changing the variable name vs changing the version argument

Changing the variable name essentially creates an entirely new entity within Chronon. This has a number of implications:

1. When computing features for the new entity, it would not reuse unchanged computation as extensively as it would with a bump to the `version` argument
2. Downstream consumers would need to explicitly migrate to the new version (as opposed to a version bump where it happens automatically)

You might want to do this if you intend to keep two versions running in production for an extended period. I.e.

```
# Warning -- to be deprecated
legacy_purchase_features = GroupBy(...)

# Use this version instead
purchase_features = GroupBy(...)
```

This would allow for both `GroupBy`s to be considered production at the same time. Choose names that describe why the entities differ.

## Best Practices

File Naming:
1. Describe the features being defined at a high level (i.e. `user_activities.py`, `payments.py`, etc.)
2. Keep file names as short as possible
3. Avoid redundancy with team name or variable name

Variable Naming:
1. Use names that describe what the entity represents
2. Keep variable names short, and avoid redundancy with the file name
3. Change the variable name when creating a separate entity, such as `baseline` and `with_recent_personalization`. Use a `version=` bump for incremental changes to the same entity.

Versioning:
1. When creating a new entity, start with `version=0`.
2. When iterating on an entity, create a branch and bump the `version=` argument
3. Iterations on your branch can keep the same version (no need to bump version in between runs while iterating on a branch)
