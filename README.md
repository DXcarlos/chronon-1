> DISCLAIMER: This is a fork of [Airbnb's Chronon repo](https://github.com/airbnb/chronon) with the **same Apache-2 license**.
> This will be free and open-source in perpetuity. It can be used and modified freely by anyone for any purpose.

Please refer to docs on our [docsite](https://zipline.ai/docs/getting_started/Introduction).

## What are the main differences of Zipline's Chronon with Airbnb's version of Chronon?

Below is a list of upgrades and major improvements. 
While some of the improvements are hard to upstream due to legacy reasons, we are supportive of anyone trying to upstream to the airbnb repo.

### Integrations

Chronon is originally built for Airbnb's data stack - Hive + Parquet + Kafka. 
To generalize it to the diverse needs of our customers who use snowflake, bigquery, redshift etc., 
we had rework the engine significantly (**without** changing any API) to support these needs.

We have native connectors to fully leverage technologies like Hudi, Iceberg, BigTable, Dynamo, Kinesis, Pub/Sub etc. 
Many in the industry are NOT on the standard path of Hive + Parquet, Kafka. 

Most of Chronon users tend to be on one of the three major clouds, AWS, GCP and Azure. Each of them provide a fully managed clustering solution
for launching spark and flink jobs like - EMR, Dataproc and Azure Spark. We added extensive support to leverage these clustering solutions as
first-class components. 

---
### Security Vulnerability prevention

The security bar for commercial use of software is much much higher than for open source projects. 
As such, whenever a new vulnerability is discovered, we need to **immediately** patch it to keep our user's systems secure.

We need to upgrade versions of core libraries - like spark, flink, thrift to their latest possible versions. 
The airbnb version of chronon, for example uses thrift 0.9 (2015), we are on 0.21 (2024). 

We detect vulnerabilities in pull requests as part of our CI to ensure that chronon deployments are resistant to known exploits.

---
### Iteration speed 

#### Build system

We drastically simplfied the build system - since we can always pick **one** (latest) version of spark & flink that all major clouds support. 
While the Airbnb fork needs to support older version due to legacy requirements. (example: Spark 2.4, 3.1 etc vs. supporting Spark 3.5)
This allows for the build and CI to be much faster. This also makes it easier to work with IDEs like intellij and contribute back.

We chose *mill* as our preferred build system over *bazel* given the poor support for the *rules_scala* extension, which prevents migration from
the deprecated WORKSPACE to the more modern bzlmod approach. Bazel is also a lot more complicated and slower even than sbt.

#### Unit tests

We have eliminated all flaky tests and reduced the CI time from 20 minutes to 4 minutes. This is a significant compounding improvement 
to the speed at which we can iterate and improve the project. It also makes it easier to onboard new contributors to the project.

---
### Observability

Since we interface deeply with table formats like iceberg, we can reach into the column statistics layers of these table formats to extract
data quality metrics in a manner that is essentially zero-cost. We produce column statistics at the end of every chronon job for free.

We also added support for more complex data quality metrics, like realtime drift detection that are only found in proprietary observability 
systems like Arize and Fiddler.

---
### Resolving a few major issues in the API.

Some of the APIs we added to Chronon at airbnb, generalized poorly to the diverse needs of ML/AI data engineering.

One example is label computation. The previous API (`LabelPart`) built on the `GroupBy` concept, but most label computation setups were too complicated for `GroupBy` to handle. Even for simpler cases, reasoning about a `GroupBy` alongside label offsets was unintuitive and often led to subtle but significant errors in training data.

We replaced this with `StagingQuery` (free-form SQL) with `recomputeDays`, which produces the recomputation behavior that label generation needs. This has been very intuitive for even novice users and an **order-of-magnitude cheaper and faster** since we don't convert parquet data into on-heap Java objects. It also supports point-in-time label attribution that many impression-to-engagement-attribution use-cases need.

---

### Optimizations

We have introduced a skew-free algorithm to compute point-in-time data that improves the training data backfill speed by 9x for most cases.
We also replaced the avro serde with LinkedIn's fastAvro library that reduces the read latency by a significant amount.
We improved the cost of processing large streams by fusing the flink operators and avoiding unfiltered traffic between operators. 
This improved the stream processing throughput by 4x.

---

