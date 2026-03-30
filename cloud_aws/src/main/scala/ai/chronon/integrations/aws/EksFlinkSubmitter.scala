package ai.chronon.integrations.aws

import ai.chronon.api.JobStatusType
import ai.chronon.spark.submission.JobSubmitterConstants.{MaxRetainedCheckpoints, additionalFlinkJars}
import io.fabric8.kubernetes.api.model.GenericKubernetesResource
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder
import io.fabric8.kubernetes.client.{Config, KubernetesClientBuilder}
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext
import org.slf4j.LoggerFactory

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

/** Submits Flink jobs to an EKS cluster via the Flink Kubernetes Operator's FlinkDeployment CRD.
  *
  * Uses IRSA (IAM Roles for Service Accounts) for AWS credential access — no explicit credentials
  * are passed; the pod's service account role is assumed automatically via web identity tokens.
  */
class EksFlinkSubmitter(k8sConfig: Option[Config] = None, ingressBaseUrl: Option[String] = None) {
  import EksFlinkSubmitter._

  private val logger = LoggerFactory.getLogger(getClass)

  // EMR on EKS Flink image
  private val FlinkImage = "public.ecr.aws/emr-on-eks/flink/emr-7.12.0-flink:latest"

  // TM memory tiers. TM sizing follows the following conventions:
  // 64G: 4 slots, tuned network/managed/metaspace settings matching prior load testing on Dataproc
  // 32G: 2 slots, halved from 64G settings
  // 16G: 1 slot, tuned settings for a smaller footprint
  // Small (anything else, e.g. 8G): 1 slot, no explicit memory tuning — Flink defaults apply,
  // and the pod resource size is taken directly from jobProperties (or Flink's own default if absent)
  private sealed trait MemoryTier {
    // None means Flink defaults
    def tmProcessMemory: Option[String]
    def jmProcessMemory: String = "4G"
    def taskSlots: Int
    def networkMin: Option[String]
    def networkMax: Option[String]
    def managedFraction: Option[String]
    def metaspaceSize: Option[String]
    def taskOffHeap: Option[String]
  }

  private case object TaskManager64G extends MemoryTier {
    val tmProcessMemory = Some("64G")
    val taskSlots = 4
    val networkMin = Some("1G")
    val networkMax = Some("2G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("1G")
  }

  private case object TaskManager32G extends MemoryTier {
    val tmProcessMemory = Some("32G")
    val taskSlots = 2
    val networkMin = Some("512m")
    val networkMax = Some("1G")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("512m")
  }
  private case object TaskManager16G extends MemoryTier {
    val tmProcessMemory = Some("16G")
    val taskSlots = 1
    val networkMin = Some("256m")
    val networkMax = Some("512m")
    val managedFraction = Some("0.5f")
    val metaspaceSize = Some("512m")
    val taskOffHeap = Some("256m")
  }

  private case object SmallTaskManager extends MemoryTier {
    val tmProcessMemory = None
    val taskSlots = 1
    val networkMin = None
    val networkMax = None
    val managedFraction = None
    val metaspaceSize = None
    val taskOffHeap = None
  }

  private def parseTmMemoryTier(jobProperties: Map[String, String]): MemoryTier = {
    jobProperties.get("taskmanager.memory.process.size") match {
      case Some(v) if v.toUpperCase.startsWith("64") => TaskManager64G
      case Some(v) if v.toUpperCase.startsWith("32") => TaskManager32G
      case Some(v) if v.toUpperCase.startsWith("16") => TaskManager16G
      case None                                      => TaskManager16G
      case _                                         => SmallTaskManager
    }
  }

  private def flinkMemoryConfig(tier: MemoryTier): Map[String, String] = {
    val base = Map(
      "jobmanager.memory.process.size" -> tier.jmProcessMemory,
      "taskmanager.numberOfTaskSlots" -> tier.taskSlots.toString
    )
    val optional = List(
      tier.tmProcessMemory.map("taskmanager.memory.process.size" -> _),
      tier.networkMin.map("taskmanager.memory.network.min" -> _),
      tier.networkMax.map("taskmanager.memory.network.max" -> _),
      tier.managedFraction.map("taskmanager.memory.managed.fraction" -> _),
      tier.metaspaceSize.map("taskmanager.memory.jvm-metaspace.size" -> _),
      tier.taskOffHeap.map("taskmanager.memory.task.off-heap.size" -> _)
    ).flatten.toMap
    base ++ optional
  }

  private[aws] def buildFlinkConfiguration(flinkCheckpointUri: String,
                                           jobProperties: Map[String, String]): Map[String, String] = {
    val tier = parseTmMemoryTier(jobProperties)

    // jobProperties applied last so callers can override any default
    // All jars are available via FLINK_CLASSPATH=/opt/flink/usrlib/*,
    // which Flink surfaces as pipeline.classpaths.
    Map(
      "state.savepoints.dir" -> flinkCheckpointUri,
      "state.checkpoints.dir" -> flinkCheckpointUri,
      "state.backend.type" -> "rocksdb",
      "state.backend.incremental" -> "true",
      // override the local dir as the default path can exceed filesystem name length limits
      "state.backend.rocksdb.localdir" -> "/tmp/flink-state",
      "state.checkpoint-storage" -> "filesystem",
      "rest.profiling.enabled" -> "true",
      "state.checkpoints.num-retained" -> MaxRetainedCheckpoints,
      // Use Web Identity Token credentials (IRSA) instead of EC2 instance metadata
      "s3.access.key" -> "",
      "s3.secret.key" -> "",
      "fs.s3a.aws.credentials.provider" -> "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
      // Expose Flink metrics via Prometheus HTTP endpoint so the AWS managed scraper can collect them.
      // Port range handles multiple TM pods co-scheduled on the same node.
      "metrics.reporters" -> "prom",
      "metrics.reporter.prom.factory.class" -> "org.apache.flink.metrics.prometheus.PrometheusReporterFactory",
      "metrics.reporter.prom.port" -> "9250-9260"
    ) ++ flinkMemoryConfig(tier) ++ jobProperties
  }

  private def flinkDeploymentCrdContext: CustomResourceDefinitionContext =
    new CustomResourceDefinitionContext.Builder()
      .withGroup("flink.apache.org")
      .withVersion("v1beta1")
      .withScope("Namespaced")
      .withPlural("flinkdeployments")
      .withKind("FlinkDeployment")
      .build()

  private def k8sClient = new KubernetesClientBuilder()
    .withConfig(k8sConfig.getOrElse(Config.autoConfigure(null)))
    .build()

  def status(deploymentName: String, namespace: String): JobStatusType = {
    val client = k8sClient
    try {
      val resource = client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .withName(deploymentName)
        .get()

      if (resource == null) return JobStatusType.UNKNOWN

      val statusMap = Option(resource.getAdditionalProperties.get("status"))
        .collect { case m: java.util.Map[_, _] => m }

      val lifecycleState = statusMap
        .flatMap(s => Option(s.get("lifecycleState")))
        .map(_.toString)
        .getOrElse("")

      val jmDeploymentStatus = statusMap
        .flatMap(s => Option(s.get("jobManagerDeploymentStatus")))
        .map(_.toString)
        .getOrElse("")

      val creationTimestamp = Option(resource.getMetadata.getCreationTimestamp)
        .map(ts => Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(ts)))

      resolveStatus(deploymentName, lifecycleState, jmDeploymentStatus, creationTimestamp)
    } finally {
      client.close()
    }
  }

  private[aws] def resolveStatus(deploymentName: String,
                                 lifecycleState: String,
                                 jmDeploymentStatus: String,
                                 creationTimestamp: Option[Instant]): JobStatusType = {
    lifecycleState match {
      case "STABLE"                                              => JobStatusType.RUNNING
      case "SUSPENDED" | "FAILED"                                => JobStatusType.FAILED
      case "DEPLOYED" | "CREATED" | "UPGRADING" | "ROLLING_BACK" =>
        // The operator sets jobManagerDeploymentStatus=ERROR for detected pod failures (e.g.
        // CrashLoopBackOff on the init container), but lifecycleState stays DEPLOYED . Similarly, unschedulable TMs
        // leave the deployment stuck in DEPLOYED indefinitely. In these cases we want to fail relatively
        // quickly so the orchestrator can surface the error and avoid long waits, but we also want to give
        // the operator some time to react to transient issues (e.g. scheduling delays due to cluster
        // autoscaling or temporary capacity shortages) before we declare failure.
        val timedOut = creationTimestamp.exists { created =>
          Instant.now().isAfter(created.plusSeconds(DeploymentPendingTimeout.toSeconds))
        }
        if (jmDeploymentStatus == "ERROR") {
          // ERROR means the operator has positively detected a pod failure (e.g. CrashLoopBackOff)
          // — safe to fail immediately regardless of age.
          logger.warn(s"FlinkDeployment $deploymentName has jobManagerDeploymentStatus=ERROR, treating as FAILED")
          JobStatusType.FAILED
        } else if (timedOut && jmDeploymentStatus == "MISSING") {
          // MISSING is the normal state immediately after creation before the JM pod starts.
          // Only treat it as a failure once the deployment has been around long enough that
          // the JM should have started by now.
          logger.warn(
            s"FlinkDeployment $deploymentName has jobManagerDeploymentStatus=MISSING after $DeploymentPendingTimeout, treating as FAILED")
          JobStatusType.FAILED
        } else if (timedOut) {
          logger.warn(
            s"FlinkDeployment $deploymentName has been in $lifecycleState for more than $DeploymentPendingTimeout without reaching STABLE, treating as FAILED")
          JobStatusType.FAILED
        } else {
          JobStatusType.PENDING
        }
      case _ =>
        logger.warn(
          s"Flink deployment $deploymentName has unrecognized lifecycleState=$lifecycleState, treating as UNKNOWN")
        JobStatusType.UNKNOWN
    }
  }

  def delete(deploymentName: String, namespace: String): Unit = {
    val client = k8sClient
    try {
      client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .withName(deploymentName)
        .delete()
      logger.info(s"Deleted FlinkDeployment: $deploymentName in namespace: $namespace")
    } finally {
      client.close()
    }
  }

  def submit(jobId: String,
             mainClass: String,
             mainJarUri: String,
             jarUris: Array[String],
             flinkCheckpointUri: String,
             maybeSavepointUri: Option[String],
             maybeFlinkJarsUri: Option[String],
             jobProperties: Map[String, String],
             args: Seq[String],
             serviceAccount: String,
             namespace: String): String = {

    val deploymentName = sanitizeDeploymentName(s"flink-$jobId")
    val basePath = maybeFlinkJarsUri.getOrElse(DefaultS3FlinkJarsBasePath)
    val flinkJars = additionalFlinkJars(basePath) ++ eksAdditionalFlinkJars(basePath)
    val allJarUris = (jarUris ++ flinkJars).distinct
    val tier = parseTmMemoryTier(jobProperties)

    val flinkConfiguration = buildFlinkConfiguration(flinkCheckpointUri, jobProperties)

    val spec = new java.util.HashMap[String, Object]()
    spec.put("image", FlinkImage)
    spec.put("flinkVersion", "v1_20")
    spec.put("serviceAccount", serviceAccount)
    spec.put("flinkConfiguration", flinkConfiguration.asJava)

    val allJars = (mainJarUri +: allJarUris).distinct
    val (initContainers, envVars, usrlibVolumeMounts, volumes) = buildUsrlibSpec(allJars)

    // Pod resource memory must match what Flink is configured to use.
    // For sized tiers (64G/32G) we set it explicitly; for small tiers we use whatever jobProperties
    // supplied, falling back to a conservative 4G default so the pod request is never left unset.
    val tmPodMemory = flinkConfiguration.getOrElse("taskmanager.memory.process.size", "4G")
    val jmPodMemory = flinkConfiguration.getOrElse("jobmanager.memory.process.size", "4G")

    spec.put("jobManager",
             buildComponentSpec(memory = jmPodMemory,
                                cpu = 1.0,
                                replicas = Some(1),
                                initContainers,
                                envVars,
                                usrlibVolumeMounts,
                                volumes))
    spec.put("taskManager",
             buildComponentSpec(memory = tmPodMemory,
                                cpu = tier.taskSlots.toDouble,
                                replicas = None,
                                initContainers,
                                envVars,
                                usrlibVolumeMounts,
                                volumes))

    val localJarUri = s"local:///opt/flink/usrlib/${mainJarUri.split("/").last}"
    val job = new java.util.HashMap[String, Object]()
    job.put("jarURI", localJarUri)
    job.put("entryClass", mainClass)
    job.put("args", args.toArray)
    // Operator validates parallelism > 0; actual job-level parallelism is set inside the Flink job itself
    job.put("parallelism", Integer.valueOf(1))
    job.put("state", "running")
    maybeSavepointUri match {
      case Some(savepointUri) =>
        job.put("upgradeMode", "savepoint")
        job.put("initialSavepointPath", savepointUri)
      case None =>
        // stateless allows cold restarts without HA; last-state requires HA to be enabled
        job.put("upgradeMode", "stateless")
    }
    spec.put("job", job)

    val client = k8sClient

    try {
      val resource = new GenericKubernetesResource()
      resource.setApiVersion("flink.apache.org/v1beta1")
      resource.setKind("FlinkDeployment")
      resource.setMetadata(
        new io.fabric8.kubernetes.api.model.ObjectMetaBuilder()
          .withName(deploymentName)
          .withNamespace(namespace)
          .build()
      )
      resource.setAdditionalProperty("spec", spec)

      val created = client
        .genericKubernetesResources(flinkDeploymentCrdContext)
        .inNamespace(namespace)
        .resource(resource)
        .create()

      logger.info(s"Created FlinkDeployment: $deploymentName in namespace: $namespace")

      if (ingressBaseUrl.isDefined) {
        try {
          createFlinkIngress(client, deploymentName, namespace, created.getMetadata.getUid)
        } catch {
          case e: Exception =>
            logger.warn(
              s"FlinkDeployment '$deploymentName' (namespace=$namespace, uid=${created.getMetadata.getUid}) " +
                s"was created successfully but ingress setup failed — Flink UI may be unavailable: ${e.getMessage}",
              e
            )
        }
      }

      deploymentName
    } finally {
      client.close()
    }
  }

  // Create an Ingress resource for the Flink REST UI, with rules specific to this deployment. This allows
  // nginx-ingress to route /flink/{deploymentName} to the correct Flink
  // REST service, and ensures the Ingress is automatically deleted when the FlinkDeployment is removed.
  private[aws] def createFlinkIngress(client: io.fabric8.kubernetes.client.KubernetesClient,
                                      deploymentName: String,
                                      namespace: String,
                                      ownerUid: String): Unit = {
    // Extract host from ingressBaseUrl so the ingress rule is host-specific, matching at the
    // same specificity level as the hub catch-all ingress. Without a host, nginx-ingress prefers
    // host-specific rules (even with path: /) over wildcard-host rules with longer paths.
    val host = ingressBaseUrl
      .flatMap { url =>
        scala.util.Try(new java.net.URI(url).getHost).toOption.filter(_ != null)
      }
      .getOrElse(throw new IllegalArgumentException(
        s"Could not extract host from ingressBaseUrl: $ingressBaseUrl — cannot create host-specific ingress rule"
      ))

    val ingress = new IngressBuilder()
      .withNewMetadata()
      .withName(deploymentName)
      .withNamespace(namespace)
      // Owner reference ensures Kubernetes GC deletes the Ingress when the FlinkDeployment
      // is removed for any reason (crash, operator cleanup, manual delete).
      .addNewOwnerReference()
      .withApiVersion("flink.apache.org/v1beta1")
      .withKind("FlinkDeployment")
      .withName(deploymentName)
      .withUid(ownerUid)
      .withController(true)
      .withBlockOwnerDeletion(true)
      .endOwnerReference()
      .addToAnnotations("nginx.ingress.kubernetes.io/use-regex", "true")
      .addToAnnotations("nginx.ingress.kubernetes.io/rewrite-target", "/$2")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-read-timeout", "3600")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-send-timeout", "3600")
      .addToAnnotations("nginx.ingress.kubernetes.io/proxy-http-version", "1.1")
      .endMetadata()
      .withNewSpec()
      .withIngressClassName("nginx-hub")
      .addNewRule()
      .withHost(host)
      .withNewHttp()
      .addNewPath()
      .withPath(s"/flink/$deploymentName(/|$$)(.*)")
      .withPathType("ImplementationSpecific")
      .withNewBackend()
      .withNewService()
      .withName(s"$deploymentName-rest")
      .withNewPort()
      .withNumber(8081)
      .endPort()
      .endService()
      .endBackend()
      .endPath()
      .endHttp()
      .endRule()
      .endSpec()
      .build()

    client.network().v1().ingresses().inNamespace(namespace).resource(ingress).create()
    logger.info(s"Created Ingress: $deploymentName in namespace: $namespace")
  }

  // Builds the init container, volume, volume mounts, and env vars for downloading JARs from S3.
  // Returns a tuple of (initContainers, envVars, volumeMounts, volumes) to be passed into buildComponentSpec.
  private def buildUsrlibSpec(jarUris: Array[String]): (
      java.util.List[java.util.Map[String, Object]],
      java.util.List[java.util.Map[String, String]],
      java.util.List[java.util.Map[String, String]],
      java.util.List[java.util.Map[String, Object]]
  ) = {
    val downloadCommands = jarUris
      .map { jar =>
        val dest = s"/opt/flink/usrlib/${jar.split("/").last}"
        s"aws s3 cp ${shellQuote(jar)} ${shellQuote(dest)}"
      }
      .mkString(" && ")

    val initContainer = new java.util.HashMap[String, Object]()
    initContainer.put("name", "download-jars")
    initContainer.put("image", "amazon/aws-cli:latest")
    val commands = new java.util.ArrayList[String]()
    commands.add("sh"); commands.add("-c"); commands.add(downloadCommands)
    initContainer.put("command", commands)
    val initVolumeMount = new java.util.HashMap[String, String]()
    initVolumeMount.put("name", "flink-usrlib")
    initVolumeMount.put("mountPath", "/opt/flink/usrlib")
    val initVolumeMounts = new java.util.ArrayList[java.util.Map[String, String]]()
    initVolumeMounts.add(initVolumeMount)
    initContainer.put("volumeMounts", initVolumeMounts)
    val initContainers = new java.util.ArrayList[java.util.Map[String, Object]]()
    initContainers.add(initContainer)

    val usrlibVolumeMount = new java.util.HashMap[String, String]()
    usrlibVolumeMount.put("name", "flink-usrlib")
    usrlibVolumeMount.put("mountPath", "/opt/flink/usrlib")
    val usrlibVolumeMounts = new java.util.ArrayList[java.util.Map[String, String]]()
    usrlibVolumeMounts.add(usrlibVolumeMount)

    val classpathEnvVar = new java.util.HashMap[String, String]()
    classpathEnvVar.put("name", "FLINK_CLASSPATH")
    classpathEnvVar.put("value", "/opt/flink/usrlib/*")
    val envVars = new java.util.ArrayList[java.util.Map[String, String]]()
    envVars.add(classpathEnvVar)

    val usrlibVolume = new java.util.HashMap[String, Object]()
    usrlibVolume.put("name", "flink-usrlib")
    usrlibVolume.put("emptyDir", new java.util.HashMap[String, Object]())
    val volumes = new java.util.ArrayList[java.util.Map[String, Object]]()
    volumes.add(usrlibVolume)

    (initContainers, envVars, usrlibVolumeMounts, volumes)
  }

  private def buildComponentSpec(
      memory: String,
      cpu: Double,
      replicas: Option[Int],
      initContainers: java.util.List[java.util.Map[String, Object]],
      envVars: java.util.List[java.util.Map[String, String]],
      volumeMounts: java.util.List[java.util.Map[String, String]],
      volumes: java.util.List[java.util.Map[String, Object]]): java.util.Map[String, Object] = {
    val component = new java.util.HashMap[String, Object]()

    val resource = new java.util.HashMap[String, Object]()
    resource.put("memory", memory)
    resource.put("cpu", Double.box(cpu))
    component.put("resource", resource)

    replicas.foreach(r => component.put("replicas", Integer.valueOf(r)))

    val mainContainer = new java.util.HashMap[String, Object]()
    mainContainer.put("name", "flink-main-container")
    mainContainer.put("env", envVars)
    mainContainer.put("volumeMounts", volumeMounts)

    val podSpec = new java.util.HashMap[String, Object]()
    podSpec.put("initContainers", initContainers)
    podSpec.put("containers", {
                  val list = new java.util.ArrayList[java.util.Map[String, Object]]()
                  list.add(mainContainer)
                  list
                })
    podSpec.put("volumes", volumes)

    val podMeta = new java.util.HashMap[String, Object]()
    podMeta.put(
      "annotations", {
        val m = new java.util.HashMap[String, String]()
        m.put("prometheus.io/scrape", "true")
        m.put("prometheus.io/port", "9250")
        m.put("prometheus.io/path", "/metrics")
        m
      }
    )

    val podTemplate = new java.util.HashMap[String, Object]()
    podTemplate.put("metadata", podMeta)
    podTemplate.put("spec", podSpec)
    component.put("podTemplate", podTemplate)

    component
  }
}

object EksFlinkSubmitter {
  // Default path for the libs we need for Spark expression eval in Flink
  val DefaultS3FlinkJarsBasePath = "s3://zipline-spark-libs/spark-3.5.3/libs/"

  // How long a deployment may stay in a non-STABLE pending state before we declare it failed.
  val DeploymentPendingTimeout: Duration = Duration(10, TimeUnit.MINUTES)

  // Jars required on EKS that other engines like Dataproc provide via their pre-installed Hadoop/YARN host classpath.
  val EksOnlyAdditionalJarNames: Array[String] = Array(
    "hadoop-client-runtime-3.3.6.jar",
    "jakarta.servlet-api-4.0.3.jar"
  )

  def eksAdditionalFlinkJars(flinkJarsBasePath: String): Array[String] = {
    val base = if (flinkJarsBasePath.endsWith("/")) flinkJarsBasePath else flinkJarsBasePath + "/"
    EksOnlyAdditionalJarNames.map(base + _)
  }

  // Flink Kubernetes Operator enforces a 45-char limit on FlinkDeployment names.
  def sanitizeDeploymentName(raw: String): String = {
    val cleaned = raw.toLowerCase
      .replaceAll("[^a-z0-9-]", "-")
      .replaceAll("^-+|-+$", "")
      .take(45)
      .replaceAll("-+$", "")
    require(cleaned.nonEmpty, "jobId must produce a valid Kubernetes name")
    cleaned
  }

  def shellQuote(s: String): String = "'" + s.replace("'", "'\"'\"'") + "'"

}
