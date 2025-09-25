package ai.chronon.online

object FetcherUtil {
  val FeatureExceptionSuffix = "_exception"

  def filterFeatureMapForErrors(featureMap: Map[String, AnyRef]): Map[String, String] = {
    featureMap.filter(_._1.endsWith(FetcherUtil.FeatureExceptionSuffix)).map(v => v._1 -> v._2.toString)
  }

}
