package ai.chronon.online.fetcher

import ai.chronon.api.Constants._
import ai.chronon.api.Extensions.StringOps
import ai.chronon.api.ScalaJavaConversions._
import ai.chronon.api.{Join, ThriftJsonCodec}
import ai.chronon.api.thrift.TBase
import ai.chronon.online.Api
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.rogach.scallop.{ScallopConf, ScallopOption, Subcommand}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.io.Source
import scala.reflect.ClassTag
import scala.reflect.internal.util.ScalaClassLoader
import scala.util.{Failure, Success, Try}

object FetcherMain {
  @transient lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  trait FetcherArgs extends ScallopConf {
    val isGcp: ScallopOption[Boolean] =
      opt[Boolean](required = false, default = Some(false), descr = "Whether to use GCP")

    val confPath: ScallopOption[String] = opt[String](required = false, descr = "Path to conf to fetch features")
    val keyJson: ScallopOption[String] = opt[String](required = false, descr = "json of the keys to fetch")
    val name: ScallopOption[String] = opt[String](required = false, descr = "name of the join/group-by to fetch")
    val confType: ScallopOption[String] =
      choice(
        Seq(JoinFolder, GroupByFolder),
        required = false,
        descr = "the type of conf to fetch",
        default = Some(JoinFolder)
      )

    val keyJsonFile: ScallopOption[String] = opt[String](
      required = false,
      descr = "file path to json of the keys to fetch",
      short = 'f'
    )
    val atMillis: ScallopOption[Long] = opt[Long](
      required = false,
      descr = "timestamp to fetch the data at",
      default = None
    )
    val interval: ScallopOption[Int] = opt[Int](
      required = false,
      descr = "interval between requests in seconds",
      default = Some(1)
    )
    val loop: ScallopOption[Boolean] = opt[Boolean](
      required = false,
      descr = "flag - loop over the requests until manually killed",
      default = Some(false)
    )

    val gcpProjectId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP project id")
    val gcpBigtableInstanceId: ScallopOption[String] =
      opt[String](required = false, descr = "GCP BigTable instance id")

    lazy private val gcpMap = Map(
      "GCP_PROJECT_ID" -> gcpProjectId.toOption.getOrElse(""),
      "GCP_BIGTABLE_INSTANCE_ID" -> gcpBigtableInstanceId.toOption.getOrElse("")
    )
    val propsInner: Map[String, String] = props[String]('Z')

    val onlineJar: ScallopOption[String] =
      opt[String](required = true,
                  name = "online-jar",
                  descr = "Path to the jar contain the implementation of Online.Api class")
    val onlineClass: ScallopOption[String] =
      opt[String](required = true,
                  descr = "Fully qualified Online.Api based class. We expect the jar to be on the class path")

    def impl(props: Map[String, String]): Api = {
      val urls = Array(new File(onlineJar()).toURI.toURL)
      val cl = ScalaClassLoader.fromURLs(urls, this.getClass.getClassLoader)
      val cls = cl.loadClass(onlineClass())
      val constructor = cls.getConstructors.apply(0)
      val onlineImpl = constructor.newInstance(props)
      onlineImpl.asInstanceOf[Api]
    }

    // hashmap implements serializable
    def serializableProps: Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      propsInner.foreach { case (key, value) => map.update(key, value) }
      map.toMap
    }

    lazy val api: Api = isGcp.toOption match {
      case Some(true) => impl(serializableProps ++ gcpMap)
      case _          => impl(serializableProps)
    }
  }

  class Args(args: Array[String]) extends ScallopConf(args) {
    object FetcherMainArgs extends Subcommand("fetch") with FetcherArgs
    addSubcommand(FetcherMainArgs)
    requireSubcommand()
    verify()
  }

  def parseConf[T <: TBase[_, _]: Manifest: ClassTag](confPath: String): T =
    ThriftJsonCodec.fromJsonFile[T](confPath, check = false)

  def run(args: FetcherArgs): Unit = {
    if (args.keyJson.isEmpty && args.keyJsonFile.isEmpty) {
      throw new Exception("At least one of keyJson and keyJsonFile should be specified!")
    }
    require(!args.confPath.isEmpty || !args.name.isEmpty, "--conf-path or --name should be specified!")
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    def readMap: String => Map[String, AnyRef] = { json =>
      objectMapper.readValue(json, classOf[java.util.Map[String, AnyRef]]).toScala
    }
    def readMapList: String => Seq[Map[String, AnyRef]] = { jsonList =>
      objectMapper
        .readValue(jsonList, classOf[java.util.List[java.util.Map[String, AnyRef]]])
        .toScala
        .map(_.toScala)
        .toSeq
    }
    val keyMapList =
      if (args.keyJson.isDefined) {
        Try(readMapList(args.keyJson())).toOption.getOrElse(Seq(readMap(args.keyJson())))
      } else {
        logger.info(s"Reading requests from ${args.keyJsonFile()}")
        val file = Source.fromFile(args.keyJsonFile())
        val mapList = file.getLines().map(json => readMap(json)).toList
        file.close()
        mapList
      }
    if (keyMapList.length > 1) {
      logger.info(s"Plan to send ${keyMapList.length} fetches with ${args.interval()} seconds interval")
    }
    val fetcher = args.api.buildFetcher(debug = true, "FetcherCLI")
    def iterate(): Unit = {
      keyMapList.foreach(keyMap => {
        logger.info(s"--- [START FETCHING for ${keyMap}] ---")

        val featureName = if (args.name.isDefined) {
          args.name()
        } else {
          args.confPath().confPathToKey
        }
        lazy val joinConfOption: Option[Join] =
          args.confPath.toOption.map(confPath => parseConf[Join](confPath))
        val startNs = System.nanoTime
        val requests = Seq(Fetcher.Request(featureName, keyMap, args.atMillis.toOption))
        val resultFuture = if (args.confType() == JoinFolder) {
          fetcher.fetchJoin(requests, joinConfOption)
        } else {
          fetcher.fetchGroupBys(requests)
        }
        val result = Await.result(resultFuture, 5.seconds)
        val awaitTimeMs = (System.nanoTime - startNs) / 1e6d

        // treeMap to produce a sorted result
        val tMap = new java.util.TreeMap[String, AnyRef]()
        result.foreach(r =>
          r.values match {
            case Success(valMap) => {
              if (valMap == null) {
                logger.info("No data present for the provided key.")
              } else {
                valMap.foreach { case (k, v) => tMap.put(k, v) }

                println(
                  s"--- [FETCHED RESULT] ---\n${objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(tMap)}")
              }
              logger.info(s"Fetched in: $awaitTimeMs ms")
            }
            case Failure(exception) => {
              exception.printStackTrace()
            }
          })
        Thread.sleep(args.interval() * 1000)

      })
    }
    iterate()
    while (args.loop()) {
      logger.info("loop is set to true, start next iteration. will only exit if manually killed.")
      iterate()
    }
  }
  def main(baseArgs: Array[String]): Unit = {
    val args = new Args(baseArgs)
    FetcherMain.run(args.FetcherMainArgs)
    System.exit(0)
  }
}
