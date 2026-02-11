package ai.chronon.integrations.cloud_gcp

import ai.chronon.spark.submission.{FlinkJob, JobSubmitterConstants, JobType, SparkJob}

/** Utility functions for Dataproc label formatting and management.
  * These utilities ensure labels conform to Dataproc requirements.
  */
object DataprocUtils {

  /** Formats a label for Dataproc by converting to lowercase and replacing invalid characters.
    * Dataproc label keys and values only allow lowercase letters, numbers, underscores, and dashes.
    * Label values must be 63 characters or less and start/end with alphanumeric character.
    *
    * @param label The label to format
    * @return Formatted label with invalid characters replaced by underscores, truncated to 63 chars if needed
    */
  def formatDataprocLabel(label: String): String = {
    // Replaces any character that is not:
    // - a letter (a-z or A-Z)
    // - a digit (0-9)
    // - a dash (-)
    // - an underscore (_)
    // with an underscore (_)
    // And lowercase.
    val formatted = label.replaceAll("[^a-zA-Z0-9_-]", "_").toLowerCase

    // GCP label values have a max length of 63 characters
    val truncated = if (formatted.length > 63) {
      // Take the last 63 chars to preserve version info at the end
      formatted.takeRight(63)
    } else {
      formatted
    }

    // Ensure starts with alphanumeric (required by Dataproc)
    val withValidStart = if (truncated.nonEmpty && !truncated.head.isLetterOrDigit) {
      truncated.dropWhile(c => !c.isLetterOrDigit)
    } else {
      truncated
    }

    // Ensure ends with alphanumeric (required by Dataproc)
    if (withValidStart.nonEmpty && !withValidStart.last.isLetterOrDigit) {
      withValidStart.reverse.dropWhile(c => !c.isLetterOrDigit).reverse
    } else {
      withValidStart
    }
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
