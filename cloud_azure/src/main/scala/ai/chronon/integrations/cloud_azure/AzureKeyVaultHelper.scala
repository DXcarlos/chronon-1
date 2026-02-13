package ai.chronon.integrations.cloud_azure

import com.azure.identity.DefaultAzureCredentialBuilder
import com.azure.security.keyvault.secrets.SecretClientBuilder
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.openssl.PEMParser
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter
import org.slf4j.LoggerFactory

import java.io.StringReader
import java.net.URI
import java.security.PrivateKey

/** Helper for fetching secrets from Azure Key Vault.
  * Used by SnowflakeImport for key pair authentication.
  */
object AzureKeyVaultHelper {
  private val logger = LoggerFactory.getLogger(getClass)

  /** Parses a full Azure Key Vault secret URI into vault URL and secret name.
    *
    * @param secretUri Full secret URI (e.g., "https://my-vault.vault.azure.net/secrets/my-secret")
    * @return Tuple of (vaultUrl, secretName)
    */
  def parseSecretUri(secretUri: String): (String, String) = {
    val uri = new URI(secretUri)
    val path = uri.getPath.stripPrefix("/").stripSuffix("/")
    val pathParts = path.split("/")

    if (pathParts.length < 2 || pathParts(0) != "secrets") {
      throw new IllegalArgumentException(
        s"Invalid Azure Key Vault secret URI: $secretUri. " +
          "Expected format: https://<vault-name>.vault.azure.net/secrets/<secret-name>"
      )
    }

    val vaultUrl = s"${uri.getScheme}://${uri.getHost}"
    val secretName = pathParts(1)

    (vaultUrl, secretName)
  }

  /** Fetches a PEM-encoded private key from Azure Key Vault using a full secret URI.
    *
    * @param secretUri Full secret URI (e.g., "https://my-vault.vault.azure.net/secrets/my-secret")
    * @return The parsed PrivateKey object
    */
  def getPrivateKeyFromUri(secretUri: String): PrivateKey = {
    val (vaultUrl, secretName) = parseSecretUri(secretUri)
    getPrivateKey(vaultUrl, secretName)
  }

  def getPrivateKeyStringFromUri(secretUri: String): String = {
    val (vaultUrl, secretName) = parseSecretUri(secretUri)
    getPrivateKeyString(vaultUrl, secretName)
  }

  /** Fetches a secret value from Azure Key Vault.
    *
    * @param vaultUrl   The Key Vault URL (e.g., "https://my-vault.vault.azure.net")
    * @param secretName The name of the secret to fetch
    * @return The secret value as a string
    */
  def getSecret(vaultUrl: String, secretName: String): String = {
    logger.info(s"Fetching secret '$secretName' from Azure Key Vault: $vaultUrl")

    val credential = new DefaultAzureCredentialBuilder().build()
    val client = new SecretClientBuilder()
      .vaultUrl(vaultUrl)
      .credential(credential)
      .buildClient()

    val secret = client.getSecret(secretName)
    logger.info(s"Successfully fetched secret '$secretName' from Azure Key Vault")
    secret.getValue
  }

  def getPrivateKeyString(vaultUrl: String, secretName: String): String = {
    getSecret(vaultUrl, secretName)
  }

  /** Fetches a PEM-encoded private key from Azure Key Vault and parses it.
    *
    * @param vaultUrl   The Key Vault URL (e.g., "https://my-vault.vault.azure.net")
    * @param secretName The name of the secret containing the PEM private key
    * @return The parsed PrivateKey object
    */
  def getPrivateKey(vaultUrl: String, secretName: String): PrivateKey = {
    val pemContent = getSecret(vaultUrl, secretName)
    parsePemPrivateKey(pemContent)
  }

  /** Parses a PEM-encoded private key string into a PrivateKey object.
    * Supports PKCS#8 format (BEGIN PRIVATE KEY).
    *
    * @param pemContent The PEM-encoded private key string
    * @return The parsed PrivateKey object
    */
  def parsePemPrivateKey(pemContent: String): PrivateKey = {
    val reader = new StringReader(pemContent)
    val pemParser = new PEMParser(reader)

    try {
      val pemObject = pemParser.readObject()
      val converter = new JcaPEMKeyConverter()

      pemObject match {
        case pkInfo: PrivateKeyInfo =>
          converter.getPrivateKey(pkInfo)
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported PEM object type: ${if (other != null) other.getClass.getName else "null"}. " +
              "Expected a PKCS#8 encoded private key (BEGIN PRIVATE KEY)."
          )
      }
    } finally {
      pemParser.close()
    }
  }
}
