---
title: "Secrets Management"
order: 11
---

# Secrets Management

Chronon provides a secrets management layer that lets you store sensitive credentials in cloud vaults and reference them from configuration — without ever embedding secret values in code or team configs.

The feature works at two levels:

- **Chronon engine** — resolves vault URIs into environment variables at driver startup, making secrets available to the running Spark job.
- **Zipline Hub** — performs the same vault resolution before job submission, and additionally interpolates resolved environment variables into Spark conf values.

## Vault URI Convention

Wherever you can set an environment variable, you can store a vault reference by appending `_VAULT_URI` to the target variable name:

```
{SECRET_NAME}_VAULT_URI = <cloud vault URI>
```

At runtime the secret is fetched and exposed as:

```
{SECRET_NAME} = <resolved secret value>
```

Supported vault backends and their URI formats:

| Backend | URI example |
|---------|-------------|
| Azure Key Vault | `https://{vault-name}.vault.azure.net/secrets/{secret-name}` |
| AWS Secrets Manager | `arn:aws:secretsmanager:{region}:{account}:secret:{name}` |
| GCP Secret Manager | `projects/{project}/secrets/{secret}` or `projects/{project}/secrets/{secret}/versions/{version}` |

GCP defaults to `latest` when no version is specified.

## Chronon Engine: Environment Variable Resolution

When `BatchNodeRunner` or `KVUploadNodeRunner` starts, it automatically calls the secret resolver on the current environment before the job executes. Any `*_VAULT_URI` variable is resolved to its secret value and the result is passed to the Online API layer.

**Example — Azure Key Vault:**

Set the vault URI before submitting the job (e.g., in your cluster environment or via spark-submit `--conf spark.executorEnv.*`):

```bash
export SNOWFLAKE_PRIVATE_KEY_VAULT_URI="https://demo-service-writer-pkey.vault.azure.net/secrets/snowflake-private-key"
```

At driver startup, the resolver detects `SNOWFLAKE_PRIVATE_KEY_VAULT_URI`, fetches the secret from Azure Key Vault, and makes `SNOWFLAKE_PRIVATE_KEY` available to the job — without it ever appearing in the submitted configuration.

**Safety rules:**

- If `SNOWFLAKE_PRIVATE_KEY` is already present in the environment, vault resolution for its `_VAULT_URI` counterpart is skipped. This allows overriding secrets locally without changing vault contents.
- Fetch failures are non-fatal: the resolver logs a warning and leaves the variable unresolved, allowing a downstream layer (e.g., Hub fails to resolve, driver tries after) to retry with its own credentials.

## Zipline Hub: Resolution + Spark Conf Interpolation

The Hub performs the same vault URI resolution step before submitting a job. It also adds a second step: interpolating resolved environment variables into Spark conf values using `{VAR_NAME}` placeholders.

This two-step chain lets a single vault URI flow all the way into a Spark configuration property:

```
OC_CREDENTIAL_VAULT_URI (vault URI in env)
  → OC_CREDENTIAL (resolved secret in env)
    → {OC_CREDENTIAL} placeholder in spark conf → actual secret value in Spark
```

### Configuration Example

The following shows a team configuration that uses this pattern. The vault URI is declared in `env`, and the placeholder is used in the Spark catalog credential:

```python
azure = Team(
    ...
    node=NodeProperties(
        env={
            # Vault URI → resolves to OC_CREDENTIAL env var
            "OC_CREDENTIAL_VAULT_URI": "https://dev-zipline-secrets.vault.azure.net/secrets/oc-catalog-credential/9dfce586bacb4c67b68514a9bdfb7a52",
            # Other non-secret env vars
            "SNOWFLAKE_JDBC_URL": "jdbc:snowflake://VEJLULX-AZURE.snowflakecomputing.com/?user=demo_batch_service&db=Demo&schema=public&warehouse=demo_wh",
        },
    ),
    conf=ConfigProperties(
        common={
            **OpenCatalogConfiguration({
                "spark.sql.catalog.spark_catalog.uri": "https://vejlulx-azure-oc.snowflakecomputing.com/polaris/api/catalog",
                # {OC_CREDENTIAL} is replaced with the resolved secret at submission time
                "spark.sql.catalog.spark_catalog.credential": "{OC_CREDENTIAL}",
                "spark.sql.catalog.spark_catalog.warehouse": "demo-v2",
            }),
        },
    ),
)
```

The Hub resolves `OC_CREDENTIAL_VAULT_URI` → `OC_CREDENTIAL`, then substitutes `{OC_CREDENTIAL}` in the Spark conf before the job is submitted. The Spark driver never sees the vault URI — only the final credential value.

### Placeholder Syntax

Placeholders follow the pattern `{VAR_NAME}` where `VAR_NAME` must consist of uppercase letters, digits, and underscores (and must start with a letter or underscore). A single conf value can contain multiple placeholders:

```python
"spark.chronon.jdbc.url": "jdbc://{DB_HOST}:{DB_PORT}/mydb"
```

Placeholders that cannot be resolved (because the corresponding env var was not set or vault resolution failed) are left as-is in the submitted configuration.

## Precedence and Layering

The two-layer design means each layer can independently retry resolution:

1. The Hub resolves vault URIs using its own service identity and injects the secrets into the env before job submission. It also performs placeholder interpolation in Spark conf.
2. The Chronon engine resolves any remaining `*_VAULT_URI` entries at driver startup using the executor's identity.

This is useful when the executor has access to secrets that the Hub service account does not (e.g., tenant-specific vaults). In that case, leave the `_VAULT_URI` variable in the env; the Hub will log a warning and leave it for the driver to resolve.

## Migrating From Hardcoded Secrets

Replace hardcoded values with the vault URI + placeholder pattern in two steps:

**Before:**
```python
env={
    "SNOWFLAKE_VAULT_URI": "https://demo-service-writer-pkey.vault.azure.net/secrets/snowflake-private-key",
},
conf={
    "spark.sql.catalog.spark_catalog.credential": "XtyCirtE0/o3pcTMdkLCh7LXVno=:i++cOG/+...",
}
```

**After:**
```python
env={
    "SNOWFLAKE_PRIVATE_KEY_VAULT_URI": "https://demo-service-writer-pkey.vault.azure.net/secrets/snowflake-private-key",
    "OC_CREDENTIAL_VAULT_URI": "https://dev-zipline-secrets.vault.azure.net/secrets/oc-catalog-credential/9dfce586bacb4c67b68514a9bdfb7a52",
},
conf={
    "spark.sql.catalog.spark_catalog.credential": "{OC_CREDENTIAL}",
}
```

Note the rename: `SNOWFLAKE_VAULT_URI` → `SNOWFLAKE_PRIVATE_KEY_VAULT_URI`. The suffix before `_VAULT_URI` must match the environment variable name you want the resolved secret to appear under.
