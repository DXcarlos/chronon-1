# Connecting with Snowflake Source Data

This runbook covers how to configure Chronon to read source data from Snowflake using a `StagingQuery` with `EngineType.SNOWFLAKE`, and how to diagnose common authentication and connectivity errors.

## Requirements

### Private Key Authentication

Snowflake connections use key pair authentication with a **PKCS#8 PEM-encoded private key** (i.e., the key file begins with `-----BEGIN PRIVATE KEY-----`). PKCS#1 keys (`-----BEGIN RSA PRIVATE KEY-----`) are not supported.

There are two ways to supply the key:

**Option 1: Environment variable**

Set `SNOWFLAKE_PRIVATE_KEY` to the full PEM content. When submitting a Spark job, pass it via Spark configuration so it's available on both the driver and executors:

```bash
--conf spark.driverEnv.SNOWFLAKE_PRIVATE_KEY="$(cat /path/to/private_key.pem)"
--conf spark.executorEnv.SNOWFLAKE_PRIVATE_KEY="$(cat /path/to/private_key.pem)"
```

**Option 2: Azure Key Vault secret (fetched at runtime)**

Store the PEM content as a secret in Azure Key Vault, then provide the secret URI in the staging query's `metaData.executionInfo.env.common`:

```python
metaData=MetaData(
    executionInfo=ExecutionInfo(
        env=Env(
            common={
                "SNOWFLAKE_VAULT_URI": "https://<vault-name>.vault.azure.net/secrets/<secret-name>"
            }
        )
    )
)
```

The URI must follow the format `https://<vault-name>.vault.azure.net/secrets/<secret-name>`. The service principal running the Spark job must have `Get` permission on the Key Vault secret.

When both `SNOWFLAKE_PRIVATE_KEY` and `SNOWFLAKE_VAULT_URI` are present, the environment variable takes precedence.

### JDBC Connection URL

Set `SNOWFLAKE_JDBC_URL` in the staging query's environment configuration. The URL must start with `jdbc:snowflake://` and include the connection parameters as query string arguments:

```
jdbc:snowflake://<account>.snowflakecomputing.com/?user=<username>&db=<database>&schema=<schema>&warehouse=<warehouse>
```

Required parameters:
- `user` — The Snowflake service principal (dedicated service account) that will run the queries. Using a service principal rather than a personal user account is strongly recommended so that credentials can be rotated independently and permissions can be scoped to exactly what Chronon needs. The private key supplied via `SNOWFLAKE_PRIVATE_KEY` or `SNOWFLAKE_VAULT_URI` must be the private key whose corresponding public key is registered for this exact user in Snowflake — the two are bound together. Providing a key that belongs to a different user will cause authentication to fail (see [Key registered under a different user](#key-registered-under-a-different-user) below).
- `db` — Target database
- `schema` — Target schema
- `warehouse` — Snowflake virtual warehouse to use for query execution

## Using a StagingQuery with Snowflake

Set `engine_type=EngineType.SNOWFLAKE` and provide both `SNOWFLAKE_JDBC_URL` and one of the key options in `metaData.executionInfo.env.common`:

```python
from ai.chronon.staging_query import StagingQuery, EngineType
from ai.chronon.api.ttypes import MetaData, ExecutionInfo, Env

v1 = StagingQuery(
    query="""
        SELECT
            user_id,
            event_ts,
            ds,
            amount
        FROM my_schema.my_events
        WHERE ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    """,
    engine_type=EngineType.SNOWFLAKE,
    startPartition="2024-01-01",
    metaData=MetaData(
        name="team/my_staging_query",
        executionInfo=ExecutionInfo(
            env=Env(
                common={
                    "SNOWFLAKE_JDBC_URL": "jdbc:snowflake://myaccount.snowflakecomputing.com/?user=svc_chronon&db=ANALYTICS&schema=PUBLIC&warehouse=COMPUTE_WH",
                    "SNOWFLAKE_VAULT_URI": "https://my-vault.vault.azure.net/secrets/snowflake-private-key",
                }
            )
        ),
        dependencies=["my_schema.my_events"],
    )
)
```

The query is executed on Snowflake, and results are written back to Chronon's table format. The template variables `{{ start_date }}` and `{{ end_date }}` are substituted with the partition range being computed.

When configured correctly, the driver logs will show the connector discovering partitions and executing queries against Snowflake:

![Successful Snowflake run](<../../images/runbooks/snowflake/Successful run.png>)

## Common Errors

### No private key configured

**Symptom**

```
Snowflake private key not found. Please provide one of the following:
  1. SNOWFLAKE_PRIVATE_KEY environment variable with PEM-encoded private key content, or
  2. SNOWFLAKE_VAULT_URI environment variable with Azure Key Vault URI (e.g., https://<vault-name>.vault.azure.net/secrets/<secret-name>)
```

![Missing private key error](../../images/runbooks/snowflake/missingPrivateKey.png)

**Cause**

Neither `SNOWFLAKE_PRIVATE_KEY` (system environment variable) nor `SNOWFLAKE_VAULT_URI` (staging query config) is present. Chronon requires at least one of them to authenticate with Snowflake.

**Resolution**

Choose one of the two options described in [Requirements](#requirements) above and ensure it is correctly set before running the job. If using the environment variable, confirm it is visible to the Spark driver process — setting it only in the shell that submits the job is not sufficient; it must be passed via `--conf spark.driverEnv.SNOWFLAKE_PRIVATE_KEY`.

---

### JDBC URL not configured

**Symptom**

```
SNOWFLAKE_JDBC_URL not found in environment. Expected format:
  jdbc:snowflake://account.snowflakecomputing.com/?user=x&db=y&schema=z&warehouse=w
```

![JDBC URL not configured error](../../images/runbooks/snowflake/NoJDBCUrl.png)

**Cause**

`SNOWFLAKE_JDBC_URL` is missing from `metaData.executionInfo.env.common` in the staging query definition.

**Resolution**

Add `SNOWFLAKE_JDBC_URL` to the staging query's environment config as shown in the [Using a StagingQuery with Snowflake](#using-a-stagingquery-with-snowflake) section.

---

### JDBC URL missing required parameters

**Symptom**

```
Warehouse missing in SNOWFLAKE_JDBC_URL
```

Or one of: `User missing in SNOWFLAKE_JDBC_URL`, `DB missing in SNOWFLAKE_JDBC_URL`, `Schema missing in SNOWFLAKE_JDBC_URL`.

![JDBC URL missing parameter error](../../images/runbooks/snowflake/MissingParameterInJDBCPretty.png)

**Cause**

`SNOWFLAKE_JDBC_URL` is set but one or more of the required query string parameters (`user`, `db`, `schema`, `warehouse`) is absent. The URL is parsed at connection time and any missing parameter causes an immediate failure.

**Resolution**

Ensure all four parameters are present in the URL:

```
jdbc:snowflake://myaccount.snowflakecomputing.com/?user=svc_chronon&db=ANALYTICS&schema=PUBLIC&warehouse=COMPUTE_WH
```

A common mistake is copying a JDBC URL from another tool that omits `warehouse` or uses different parameter names (e.g., `database` instead of `db`). Chronon expects exactly `user`, `db`, `schema`, and `warehouse`.

---

### Malformed Key Vault URI

**Symptom**

The job fails at the **orchestrator level** before the Spark driver starts, so the error appears in the workflow submission notes rather than in Spark driver logs. This makes it take longer to surface than most other configuration errors.

```
Job submission failed ... Error: Status code 404,
  {"error":{"code":"SecretNotFound","message":"A secret with (name/id) <secret-name>
   was not found in this key vault."}}
```

![Malformed Key Vault URI error](../../images/runbooks/snowflake/MalformedSecretURI.png)

**Cause**

`SNOWFLAKE_VAULT_URI` is set but the URI does not conform to the expected structure. The path must contain `/secrets/<secret-name>` — URIs pointing to a key (e.g., `.../keys/<name>`), a certificate, or the vault root will fail. Because the vault lookup is attempted by the orchestrator before job submission, the failure manifests as a 404 SecretNotFound rather than a local validation error, and the job submission itself times out rather than failing immediately.

**Resolution**

Construct the URI from the vault name and secret name:

```
https://<vault-name>.vault.azure.net/secrets/<secret-name>
```

Note that version-pinned URIs (which include a version suffix like `.../secrets/<name>/<version-id>`) are also accepted, but the `/secrets/` segment must be present. Verify the URI against the Azure portal or CLI:

```bash
az keyvault secret show --vault-name <vault-name> --name <secret-name> --query id -o tsv
```

---

### Snowflake support not available (missing classpath dependency)

**Symptom**

```
java.lang.UnsupportedOperationException: Snowflake support not available.
  Make sure cloud_azure module is on the classpath.
```

**Cause**

The `cloud_azure` module JAR (which contains `SnowflakeImport`) is not present on the Spark job's classpath. The Snowflake staging query runner is loaded dynamically via reflection, so its absence is only detected at runtime when the `StagingQuery` with `EngineType.SNOWFLAKE` is first executed.

**Resolution**

Ensure the `cloud_azure` assembly JAR is included when submitting the Spark job:

```bash
--jars /path/to/cloud_azure-assembly.jar
```

Or, if using a fat jar that bundles all modules, confirm that `ai.chronon.integrations.cloud_azure.SnowflakeImport` is present in the jar:

```bash
jar tf your-assembly.jar | grep SnowflakeImport
```

---

### Wrong account identifier in JDBC URL

**Symptom**

```
JDBC driver encountered communication error. Message: HTTP status=404.
```

The error appears as an HTTP 404 from Snowflake's routing layer. An `UnknownHostException` or connection timeout is also possible if the hostname does not resolve at all.

![Wrong account identifier error](../../images/runbooks/snowflake/WrongHostInJDBC.png)

**Cause**

The account identifier in the JDBC URL does not resolve to a valid Snowflake endpoint. Snowflake supports two identifier formats and they are not interchangeable:

- **Organization format** (preferred): `orgname-accountname` → `https://orgname-accountname.snowflakecomputing.com`
- **Legacy format**: `accountname.region.cloudprovider` → `https://accountname.region.cloudprovider.snowflakecomputing.com`

Using a legacy-style name where the organization format is expected (or vice versa) results in a hostname that does not exist.

**Resolution**

Find the correct account identifier from the Snowflake UI under **Admin → Accounts**, or via the CLI:

```sql
SELECT CURRENT_ACCOUNT(), CURRENT_ORGANIZATION_NAME();
```

The JDBC URL account segment should match the locator shown there. If your organization uses the new format, the URL looks like:

```
jdbc:snowflake://myorg-myaccount.snowflakecomputing.com/?user=...
```

---

### Invalid private key

**Symptom**

```
Input PEM private key is invalid
```

![Invalid private key error](<../../images/runbooks/snowflake/Invalid Private Key (not a private key at all).png>)

**Cause**

The key provided is not in PKCS#8 unencrypted format. Common causes:
- The key is PKCS#1 format (`-----BEGIN RSA PRIVATE KEY-----`) — needs to be converted
- The key is encrypted with a passphrase (`-----BEGIN ENCRYPTED PRIVATE KEY-----`) — Chronon does not support passphrase-protected keys
- The PEM content is corrupt or truncated

Keys with or without the `-----BEGIN PRIVATE KEY-----` / `-----END PRIVATE KEY-----` header lines are both accepted, and leading/trailing whitespace and newlines within the key value are handled correctly.

**Resolution**

Convert an existing RSA key to unencrypted PKCS#8:

```bash
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
  -in rsa_private_key.pem -out private_key_pkcs8.pem
```

Verify the result starts with `-----BEGIN PRIVATE KEY-----` before storing it.

---

### Key does not have access to Snowflake

**Symptom**

```
net.snowflake.client.jdbc.SnowflakeSQLException: JWT token is invalid.
```

Or:

```
net.snowflake.client.jdbc.SnowflakeSQLException: User: <username>. Snowflake Native OAuth failed.
```

![JWT / wrong key error](../../images/runbooks/snowflake/RealKeyButWrongKey.png)

**Cause**

The private key is structurally valid but Snowflake rejected the authentication. This typically means:
- The public key has not been registered for the Snowflake user, or a different key pair was registered
- The key was rotated in Snowflake and the old key is no longer active
- There is a mismatch between the `user` in the JDBC URL and the user the key is registered under

**Resolution**

Verify that the public key fingerprint in Snowflake matches the key you are using:

```sql
DESC USER svc_chronon;
-- Check RSA_PUBLIC_KEY_FP or RSA_PUBLIC_KEY_2_FP
```

To register or update the key for a user:

```sql
ALTER USER svc_chronon SET RSA_PUBLIC_KEY='<base64-encoded-public-key>';
```

---

### Key registered under a different user

**Symptom**

Same as above — Snowflake rejects the JWT:

```
net.snowflake.client.jdbc.SnowflakeSQLException: JWT token is invalid.
```

**Cause**

The private key is structurally valid and correctly formatted, but it was generated for a different service principal than the one named in the `user` parameter of the JDBC URL. Snowflake's key pair authentication works by verifying that the JWT signature matches the public key registered for the specified `user`. If the key belongs to user `svc_other` but `user=svc_chronon` is in the URL, Snowflake looks up `svc_chronon`'s registered public key, finds it doesn't match the incoming JWT, and rejects the connection. **There is no fallback** — Snowflake will not attempt to authenticate as the key's actual owner.

This scenario is easy to hit when rotating keys or sharing secrets across teams: the key stored in the vault or environment variable was updated to one belonging to a different service account without updating the `user` in `SNOWFLAKE_JDBC_URL` (or vice versa).

**Resolution**

Ensure the `user` in `SNOWFLAKE_JDBC_URL` and the private key always refer to the same service principal. To verify which public key is registered for a given user:

```sql
DESC USER svc_chronon;
-- Check RSA_PUBLIC_KEY_FP and RSA_PUBLIC_KEY_2_FP
```

Extract the fingerprint of the key you are actually providing to confirm they match:

```bash
openssl rsa -in private_key_pkcs8.pem -pubout 2>/dev/null | \
  openssl dgst -sha256 -binary | base64
```

If the fingerprints differ, either register the correct public key for the intended user or update the `user` in the JDBC URL to match the owner of the key.

---

### Service principal lacks access to the database or table

**Symptom**

Authentication succeeds but the query fails with an authorization error. At the database level this typically appears as:

```
SQL compilation error:
  Database 'MY_DATABASE' does not exist or not authorized.
```

At the table level:

```
net.snowflake.client.jdbc.SnowflakeSQLException: SQL access control error:
  Insufficient privileges to operate on table 'MY_TABLE'
```

![Insufficient permissions error](../../images/runbooks/snowflake/InsufficientPerms.png)

**Cause**

The service principal authenticated successfully (the JWT was accepted), but the Snowflake role active for that user does not have the necessary privileges on the object being queried. In Snowflake, privileges are granted to roles, not directly to users. The default role assigned to the service principal may have no grants on the target database, schema, or table.

**Resolution**

Grant the required privileges to the role used by the service principal. At minimum, a read-only staging query needs `USAGE` on the database and schema and `SELECT` on the table:

```sql
GRANT USAGE ON DATABASE my_database TO ROLE chronon_role;
GRANT USAGE ON SCHEMA my_database.my_schema TO ROLE chronon_role;
GRANT SELECT ON TABLE my_database.my_schema.my_table TO ROLE chronon_role;
```

To verify what role the service principal is currently using and what privileges it holds:

```sql
DESC USER svc_chronon;           -- check DEFAULT_ROLE
SHOW GRANTS TO ROLE chronon_role;
```

If the JDBC URL does not specify a `role` parameter, Snowflake uses the user's `DEFAULT_ROLE`. You can either update the default role for the service principal or add a `role` parameter to the JDBC URL:

```
jdbc:snowflake://myaccount.snowflakecomputing.com/?user=svc_chronon&role=chronon_role&db=ANALYTICS&schema=PUBLIC&warehouse=COMPUTE_WH
```

---

### Secret does not exist in Key Vault

**Symptom**

```
com.azure.core.exception.ResourceNotFoundException: Secret not found: <secret-name>
  Status code 404, ...
```

**Cause**

No secret with the given name exists in the specified vault. This can happen if:
- The secret name in `SNOWFLAKE_VAULT_URI` is misspelled
- The secret was deleted or is in a soft-deleted state
- The vault name in the URI is incorrect and points to a different vault

**Resolution**

Verify the secret exists:

```bash
az keyvault secret show --vault-name <vault-name> --name <secret-name>
```

If the secret was soft-deleted, recover it:

```bash
az keyvault secret recover --vault-name <vault-name> --name <secret-name>
```

Double-check the full URI in your staging query configuration matches the actual vault and secret name exactly.

---

### Secret exists but service principal lacks access

**Symptom**

```
com.azure.core.exception.HttpResponseException: Status code 403, ...
  {"error":{"code":"Forbidden","message":"The user, group or application ... does not have secrets get permission on key vault ..."}}
```

**Cause**

The secret exists in Key Vault, but the managed identity or service principal running the Spark job does not have the `Get` permission for secrets on that vault. The `DefaultAzureCredential` used by Chronon will authenticate as the identity assigned to the compute environment (e.g., the AKS pod's workload identity or the VM's managed identity).

**Resolution**

Grant the `Key Vault Secrets User` role (or a classic `Get` access policy) to the service principal:

```bash
# RBAC-based vaults
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <service-principal-object-id> \
  --scope /subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name>

# Access policy-based vaults
az keyvault set-policy \
  --name <vault-name> \
  --object-id <service-principal-object-id> \
  --secret-permissions get
```

Confirm which identity is being used by checking the Spark executor logs for the `DefaultAzureCredential` chain resolution output, or by inspecting the managed identity attached to your compute resource.
