---
title: "Naming and Versioning"
order: 8
---


# Naming and Versioning

## Naming

`GroupBy`s, `Staging Queries`s and `Join`s are all identified by three components:

1. **Team**: Files are organized inside a team directory. I.e. `group_bys/my_team/...`
2. **File path**: There may be subdirectories within the team directory, or files directly. I.e. `group_bys/my_team/user_features.py`
3. **Variable name**: Within the file, the entity is assigned to a Python variable. I.e. `purchase_features = GroupBy(...)`. See more details below.
4. **Version**: This is an argument to the constructor. I.e. `GroupBy(..., version=0)`. See more details below.

These four components combine to fully identify an entity. I.e. `my_team.user_features.purchase_features__0`.

This will be the name that is used for fetching features, and also corresponds to the output table name for backfilled data (`my_team_user_features_purchase_features__0`).


## Variable naming

Use a descriptive variable name for the feature set or training set being defined. Avoid naming the Python variable only `v0`, `v1`, etc.; those names are easy to confuse with the separate `version=` argument.

**When should the variable name change?**

Generally, keep the same descriptive variable name and bump `version=` when you're iterating on an existing `GroupBy`, `StagingQuery`, or `Join`. Use a different variable name when you intentionally want a separate entity to exist side-by-side.

For example, say you have features defined on user activities, and you want one set that filter out bot traffic and one that doesn't.

Inside your `user_activities.py` file you might define:

```
human_activity_features = GroupBy(...) # Includes a filter clause on the source

all_activity_features = GroupBy(...) # No filter clause
```

## Versioning

`GroupBy`s, `Staging Queries`s and `Join`s all take a `version: int` argument in their constructor, i.e.:

```
purchase_features = GroupBy(
    ...
    version=0
)
```

When compiling, the version becomes part of the name of the entity as a `__{v}` suffix. In this example it would be `{team}.{file}.purchase_features__0`.

### Developer flow for iterating on versions

The version argument is helpful When making a change to an existing entity. For example, say you want to add some features to a `GroupBy`. A user would:

1. Make a git branch on their Chronon directory
2. Bump the version on their GroupBy, and add their new feature definitions
3. Test it using the Zipline/Chronon CLI (see docs on testing)
4. (Optionally) Deploy the branch with the new version for A/B testing (see docs on deploying)
5. Merge the branch to production

Doing this with a new version vs with an entirely new entity has a number of benefits:

1. Compute reuse: When running a backfill with your new `GroupBy` or `Join`, only the new features are computed. The unchanged ones are reused from existing backfills wherever possible.
2. Downstream consumers that reference that entity will automatically migrated to the new version when your merge to main. Schema incompatibilities caused by changing/removing features are caught at compile time.


## Changing the variable name vs changing the version argument

Changing the variable name essentially creates an entirely new entity within Chronon. This has a number of implications:

1. When computing features for the new entity, it would not reuse unchanged computation as extensively as it would with a bump to the `version` argument
2. Downstream consumers would need to explicitly migrate to the new version (as opposed to a version bump where it happens automatically)

You might want to do this if you intend to keep two versions running in production for an extended period. I.e.

```
# Warning -- to be deprecated
legacy_purchase_features = GroupBy(...)

# Use this version instead
expanded_purchase_features = GroupBy(...)
```

This would allow for both `GroupBy`s to be considered production at the same time.

## Best Practices

File Naming:
1. Describe the features being defined at a high level (i.e. `user_activities.py`, `payments.py`, etc.)
2. Keep file names as short as possible
3. Avoid redundancy with team name or variable name

Variable Naming:
1. Use a short descriptive name for what the entity produces, such as `purchase_features` or `training_join`.
2. Avoid redundancy with the file name, but prefer clarity over version-only names like `v1`.
3. Only change the variable name if you want to treat it as a net new entity; otherwise use a version bump for incremental changes.

Versioning:
1. When creating a new entity, start with `version=0`.
2. When iterating on an entity, create a branch and bump the version
3. Iterations on your branch can keep the same version (no need to bump version in between runs while iterating on a branch)
