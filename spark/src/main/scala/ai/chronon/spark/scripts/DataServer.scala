package ai.chronon.spark.scripts

import ai.chronon.api.SerdeUtils
import ai.chronon.api.thrift.TBase
import ai.chronon.observability.TileDriftSeries
import ai.chronon.observability.TileSeriesKey
import ai.chronon.observability.TileSummarySeries
import ai.chronon.online.stats.DriftStore
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil

import java.util.Base64
import scala.reflect.ClassTag

class DataServer(driftSeries: Seq[TileDriftSeries], summarySeries: Seq[TileSummarySeries], port: Int = 8181) {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)
  private val bossGroup = new NioEventLoopGroup(1)
  private val workerGroup = new NioEventLoopGroup()
  private val mapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .enable(SerializationFeature.INDENT_OUTPUT)

  private class HttpServerHandler extends SimpleChannelInboundHandler[HttpObject] {
    override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
      ctx.flush()
    }

    private def convertToBytesMap[T <: TBase[_, _]: Manifest: ClassTag](
        series: T,
        keyF: T => TileSeriesKey): Map[String, String] = {
      val serializerInstance = SerdeUtils.compactSerializer.get()
      val encoder = Base64.getEncoder
      val keyBytes = serializerInstance.serialize(keyF(series))
      val valueBytes = serializerInstance.serialize(series)
      Map(
        "keyBytes" -> encoder.encodeToString(keyBytes),
        "valueBytes" -> encoder.encodeToString(valueBytes)
      )
    }

    override def channelRead0(ctx: ChannelHandlerContext, msg: HttpObject): Unit = {
      msg match {
        case request: HttpRequest =>
          val uri = request.uri()

          val start = System.currentTimeMillis()
          val (status, content) = uri match {
            case "/health" =>
              (HttpResponseStatus.OK, """{"status": "healthy"}""")

            case "/api/drift-series" =>
              // val dtos = driftSeries.map(d => convertToBytesMap(d, (tds: TileDriftSeries) => tds.getKey))
              (HttpResponseStatus.OK, mapper.writeValueAsString(driftSeries))

            case "/api/summary-series" =>
              val dtos = summarySeries.map(d => convertToBytesMap(d, (tds: TileSummarySeries) => tds.getKey))
              (HttpResponseStatus.OK, mapper.writeValueAsString(dtos))

            case "/api/metrics" =>
              val metrics = Map(
                "driftSeriesCount" -> driftSeries.size,
                "summarySeriesCount" -> summarySeries.size
              )
              (HttpResponseStatus.OK, mapper.writeValueAsString(metrics))

            case _ =>
              (HttpResponseStatus.NOT_FOUND, """{"error": "Not Found"}""")
          }
          val end = System.currentTimeMillis()
          logger.info(s"Request $uri took ${end - start}ms, status: $status, content-size: ${content.length}")

          val response = new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            status,
            Unpooled.copiedBuffer(content, CharsetUtil.UTF_8)
          )

          response
            .headers()
            .set(HttpHeaderNames.CONTENT_TYPE, "application/json")
            .set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes())

          if (HttpUtil.isKeepAlive(request)) {
            response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE)
          }

          ctx.write(response)
        case _ =>
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
      cause.printStackTrace()
      ctx.close()
    }
  }

  def start(): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            val p = ch.pipeline()
            p.addLast(new HttpServerCodec())
            p.addLast(new HttpObjectAggregator(65536))
            p.addLast(new HttpServerHandler())
          }
        })
        .option[Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

      val f = b.bind(port).sync()
      println(s"Server started at http://localhost:$port/metrics")
      f.channel().closeFuture().sync()
    } finally {
      shutdown()
    }
  }

  private def shutdown(): Unit = {
    workerGroup.shutdownGracefully()
    bossGroup.shutdownGracefully()
  }
}
