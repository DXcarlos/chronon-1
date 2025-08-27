# Testing `GroupBy`s, `Join`s and `StagingQuery`s

This doc assumes that you have already a `GroupBy`, `Join`, or `StagingQuery` created that you wish to test.

## Compile

Compiling takes your python files, validates them for correctness, and produces serialized files that you can use in futures steps.

From the root of the main Chronon directory, run:

```bash
zipline compile
```

This will show you errors if there are any in your definitions, and show you changes.

Note, if you are making a change to any existing entities **without** changing the version it will prompt you with a warning.

## Backfill

```sh
zipline hub backfill --conf compiled/{group_bys|staging_queries|joins}/{team}/{your_conf} --start-ds {YYYY-MM-dd} --end-ds {YYYY-MM-dd}
```

This will give you a URL to track the progress of your backfill.

You can also see your previously run jobs in the `home` page of the Zipline Hub UI.

Generally, it's suggested to run a small range of data first to perform validation, then proceed with the entire desired range.

Once the job is complete you can query your table to see the computed data (note the table name is in the overview section of the Zipline Hub UI for convenience).

## (Optional) Schedule

Running `schedule` from a branch will tell Zipline to run the regular jobs associated with your config.

**Note:** All configs on the `main` branch are automatically scheduled. You only need to run `schedule` manually from a branch if you want to schedule runs on a config **without** merging it to `main`.

This is helpful when you want to run an extended A/B test or experiment with the data pipeline. However, if you want to skip this step you can deploy the config to production simply by merging your PR.

```sh
zipline hub schedule --conf compiled/{group_bys|staging_queries|joins}/{team}/{your_conf}
```

This will run:
1. Frontfill jobs into the output table (if the `offline_schedule` argument is set on your entity)
2. Batch upload jobs for serving (if `online=True`)
3. Streaming jobs (if a topic is configured for the `GroupBy` being scheduled)

