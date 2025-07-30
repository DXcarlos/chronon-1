package ai.chronon.spark.submission

trait StorageClient {

  /** Downloads an object from the storage to memory and converts it to the specified type.
    *
    * @param bucketName The name of the bucket.
    * @param objectName The name of the object.
    * @return The converted object.
    */
  def downloadObjectToMemory(bucketName: String, objectName: String): Array[Byte]

  /** Downloads an object from the storage to memory and converts it to the specified type.
    *
    * @param objectPath The path of the object in the storage.
    * @return The converted object.
    */
  def downloadObjectToMemory(objectPath: String): Array[Byte]

  /** Lists all files in the specified bucket.
    *
    * @param bucketName The name of the bucket.
    * @return A list of file names in the bucket.
    */
  def listFiles(bucketName: String, prefix: String): Iterator[String]

  /** Lists all files in the specified bucket.
    *
    * @param bucketPath The path of the bucket.
    * @return A list of file names in the bucket.
    */
  def listFiles(bucketPath: String): Iterator[String]

  /** Checks if a file exists in the specified bucket.
    *
    * @param bucketName The name of the bucket.
    * @param objectName The name of the object.
    * @return True if the file exists, false otherwise.
    */
  def fileExists(bucketName: String, objectName: String): Boolean

  /** Checks if a file exists in the specified bucket.
    *
    * @param objectPath The path of the object in the storage.
    * @return True if the file exists, false otherwise.
    */
  def fileExists(objectPath: String): Boolean

}
