package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.submission.{FlinkJob, JobSubmitterConstants, JobType, SparkJob}

/** Utility functions for Dataproc label formatting and management.
  * These utilities ensure labels conform to Dataproc requirements.
  */
object DataprocUtils {

  /** Formats a label for Dataproc by converting to lowercase and replacing invalid characters.
    * Dataproc label keys and values only allow lowercase letters, numbers, underscores, and dashes.
    *
    * @param label The label to format
    * @return Formatted label with invalid characters replaced by underscores
    */
  def formatDataprocLabel(label: String): String = {
    // Replaces any character that is not:
    // - a letter (a-z or A-Z)
    // - a digit (0-9)
    // - a dash (-)
    // - an underscore (_)
    // with an underscore (_)
    // And lowercase.
    label.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase
  }

  /** Creates formatted Dataproc labels from submission properties and additional labels.
    * Adds standard labels (job-type, metadata-name, zipline-version) and formats all labels
    * according to Dataproc requirements.
    *
    * @param jobTypeLabel The job type label value (e.g., "spark", "flink", "spark-serverless")
    * @param submissionProperties Submission properties containing metadata name and zipline version
    * @param additionalLabels Additional custom labels to include
    * @return Map of formatted labels ready for Dataproc API
    */
  def createFormattedDataprocLabels(jobType: JobType,
                                    submissionProperties: Map[String, String],
                                    additionalLabels: Map[String, String] = Map.empty): Map[String, String] = {
    val metadataName = submissionProperties.getOrElse(JobSubmitterConstants.MetadataName, "")
    val ziplineVersion = submissionProperties.getOrElse(JobSubmitterConstants.ZiplineVersion, "")

    val baseLabels = Map(
      JobSubmitterConstants.JobType -> (jobType match {
        case SparkJob => JobSubmitterConstants.SparkJobType
        case FlinkJob => JobSubmitterConstants.FlinkJobType
      }),
      JobSubmitterConstants.MetadataName -> metadataName,
      JobSubmitterConstants.ZiplineVersion -> ziplineVersion
    ).filter(_._2.nonEmpty) // Only include labels with non-empty values

    (baseLabels ++ additionalLabels)
      .map(entry => (formatDataprocLabel(entry._1), formatDataprocLabel(entry._2)))
  }
}
