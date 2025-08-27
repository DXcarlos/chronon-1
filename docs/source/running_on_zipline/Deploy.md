# Deploying

Once you have authored and tested your `GroupBy`, `Join` or `StagingQuery`, then all you need to do to productionize it is merge it into your repository.

Zipline Hub will automatically ingest all new entities and schedule their associated jobs.

You can track the status of each entity's jobs from the Zipline Hub UI.

## GroupBy

`GroupBy`s will get the following jobs:

1. (if `online=True`) Batch upload updates the online KV store with feature values for serving
2. (if `online=True` and a streaming topic is configured) Streaming updates to the online KV store for serving
3. (if `offline_schedule` is set) Batch snapshots that write partitions into the output table

## Join

`Join`s will get the following jobs:

1. (if `online=True`) A metadata upload job that informs the fetcher of which features are part of this join (beyond that the actual feature computation is scheduled by the `GroupBy`s used within the `Join`, however, these can also be tracked on the `Join`s page in the Zipline Hub UI)
2. (if `offline_schedule` is set) Batch jobs that frontfill data into the most recent partitions of the output table

## StagingQuery

`StagingQuery`s will get a a batch job that writes data into the most recent partition of the output table.
