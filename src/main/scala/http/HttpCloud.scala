package codecraft.platform.http

import akka.actor._
import akka.pattern.ask
import codecraft.codegen._
import codecraft.platform._
import codecraft.registry._
import java.net.InetAddress
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util._
import scala.util.Random
import scalaj.http._
import unfiltered.netty.async.Plan
import unfiltered.netty.ServerErrorResponse

@io.netty.channel.ChannelHandler.Sharable
private[http] case class HttpCloudServer(
  system: ActorSystem,
  httpCloud: HttpCloud
) extends Plan with ServerErrorResponse {
  import unfiltered.netty.{ReceivedMessage, Http}
  import unfiltered.request._
  import unfiltered.response._

  import system.dispatcher

  def start(port: Int) = Future {
    Http(port)
      .handler(this)
      .run { s =>
        println(s"Started server on ${s}")
      }
  }

  def intent = {
    case req @ POST(Path(Seg(cmdKey::Nil))) =>
      Try {
        //val payload = Body.bytes(req)
        val info = httpCloud.routingInfo cmd cmdKey
        val group = info.group
        val consumer = httpCloud.cmdConsumers get group.queueName getOrElse {
          throw new Exception(s"No command consumer for $cmdKey")
        }

        val payload = Body bytes req
        val cmd = info deserializeCmd payload get
        val method = consumer methodRegistry info.key
        val reply = method(cmd)
        val replyPayload = info serializeReply reply get

        replyPayload
      }.transform(
        data => Try {
          println(s"Handled cmd $cmdKey")
          req.respond (Ok ~> ResponseHeader("Content-Type", Set("text/plain")) ~> ResponseBytes(data))
        },
        e => Try {
          e.printStackTrace()
          req respond (BadRequest ~> ResponseHeader("Content-Type", Set("text/plain")) ~> ResponseString(s"Error: $e"))
        }
      )
  }
}

private[http] class HttpCloud(
  val routingInfo: RoutingInfo,
  system: ActorSystem,
  registryProvider: ICloud
) extends ICloud {
  import system.dispatcher

  val port = Random.nextInt(10000) + 10000

  var cmdRegistries = Map.empty[String, Registry]
  // Map of cmdKey -> Registry list
  var registryRoutes = Map.empty[String, List[Registry]]
  var cmdConsumers = Map.empty[String, CmdGroupConsumer]

  val server = HttpCloudServer(system, this)
  server.start(port)

  def sync[A](lock: Object)(action: => A): A = {
    lock synchronized action
  }

  def sync2[A](lockA: Object, lockB: Object)(action: => A): A = {
    lockA synchronized {
      lockB synchronized {
        action
      }
    }
  }

  def refreshRegistry = {
    println(s"Refreshing registries...")
    for ((cmdKey, _) <- routingInfo.cmd) {
      println(s"Listing registries for $cmdKey")
      Await result (
        registryProvider requestCmd ("cmd.registry.get", GetRegistries("http", cmdKey), 5 seconds),
        5 seconds
      ) match {
        case GetRegistriesReply(Some(registries), _, _) =>
          println(s"Got registries $registries")
          sync(registryRoutes) {
            registries map { registry =>
              val newRegistryRoutes = registry.routes map { route =>
                val existing = registryRoutes get route.cmdKey getOrElse List.empty
                (route.cmdKey -> (registry::existing))
              }
              registryRoutes ++= newRegistryRoutes
            }
          }
      }
    }
  }
  refreshRegistry

  registryProvider subscribeEvent ("event.registry.updated", {
    case RegistryUpdated(_, registry, _) =>
      println(s"Got updated registry $registry")
      sync(registryRoutes) {
        val newRegistryRoutes = registry.routes map { route =>
          val existing = registryRoutes get route.cmdKey getOrElse List.empty
          (route.cmdKey -> (registry::existing))
        }
        registryRoutes ++= newRegistryRoutes
      }
  })

  def issueCommand(data: Array[Byte], info: CmdRegistry, registry: Registry): Any = {
    Try {
      val url = s"http://${registry.host}:${registry.port}/${info.key}"
      val result = Http(url)
        .postData(data)
        .header("Content-Type", "application/json")
        .asBytes

      result match {
        case HttpResponse(body, code, _) if code >= 200 && code < 300 =>
          Some(info deserializeReply body)
        case HttpResponse(body, code, _) =>
          throw new Exception(s"CloudResponseError $code: ${new String(body)}")
      }
    } recover {
      case e: java.net.ConnectException =>
        // Refresh the registry.
        println(s"Deleting bad registry $registry")
        Await result (
          registryProvider requestCmd ("cmd.registry.delete", DeleteRegistry(registry.id), 5 seconds),
          20 seconds
        ) match {
          case DeleteRegistryReply(code, _) if code >= 200 && code < 300 => None
          case reply =>
            println(s"Got unexpected response $reply")
            throw new Exception("Unexpected reply")
        }
        refreshRegistry

        None
    } get
  }

  def requestCmd(cmdKey: String, cmd: Any, timeoutDur: FiniteDuration): Future[Any] = Future {
    // Get the routing information for this command.
    val info = routingInfo cmd cmdKey
    // Serialize the command.
    val data = info serializeCmd cmd get
    // Get a random registry to hit.
    val registriesOp = sync(registryRoutes) {
      registryRoutes get cmdKey
    }

    registriesOp getOrElse List.empty match {
      case Nil =>
        throw new Exception(s"CloudRequestError: Consumer for $cmdKey is not available")

      case registries =>
        val registry = registries ((Random.nextDouble * registries.length) toInt)

        // Now, hit this registry with an http request.
        issueCommand(data, info, registry) match {
          case None => requestCmd(cmdKey, cmd, timeoutDur)
          case Some(value) => value
        }
    }
  }

  def subscribeCmd(cmdGroupKey: String, service: CmdGroupConsumer, timeoutDur: FiniteDuration): Future[Unit] = Future {
    println(s"Subscribing service to command $cmdGroupKey")
    val groupInfo = routingInfo group cmdGroupKey
    val postRegistry = PostRegistry(
      true,
      "http",
      service.methodRegistry.map {
        case (cmdKey, _) =>
          val urlPath = cmdKey.replaceAll(".", "/")
          CmdRoute(
            urlPath,
            cmdKey
          )
      } toList,
      InetAddress.getLocalHost.getHostAddress,
      port
    )

    Await result (
      registryProvider requestCmd (
        "cmd.registry.put",
        postRegistry,
        5 seconds
      ),
      5 seconds
    ) match {
      case PostRegistryReply(Some(id), _, _) =>
        // Setup a listener for this id.
        registryProvider subscribeEvent ("event.registry.updated", {
          case RegistryUpdated(_id, _, false) =>
            subscribeCmd (
              cmdGroupKey,
              service,
              timeoutDur
            )
          case _ => ()
        })
      case PostRegistryReply(_, code, error) =>
        throw new Exception(s"RegistryError $code: $error")
    }

    println(s"Adding consumer $service to consumers")
    sync(cmdConsumers) {
      cmdConsumers += (groupInfo.queueName -> service)
    }
  }

  def disconnect() = {
    registryProvider disconnect
  }

  def publishEvent(eventKey: String,event: Any): scala.concurrent.Future[Unit] = {
    registryProvider publishEvent (eventKey, event)
  }

  def subscribeEvent(eventKey: String,method: Any => Unit): scala.concurrent.Future[Unit] = {
    registryProvider subscribeEvent (eventKey, method)
  }
}

object HttpCloud {
  def apply(system: ActorSystem, endPoint: String, routingInfo: RoutingInfo): ICloud = {
    // This registry and cloud will be used to discovery applications.
    val registryRoutingInfo = RoutingInfo(
      List(
        RegistryRoutingGroup cmdInfo
      ).flatten.map(reg => (reg.key -> reg)).toMap ++ routingInfo.cmd,
      List(
        RegistryRoutingInfo eventInfo
      ).flatten.map(info => (info.key -> info)).toMap ++ routingInfo.event,
      Map(
        RegistryRoutingGroup.groupRouting.queueName -> RegistryRoutingGroup.groupRouting
      )
    )
    // The amqp cloud will be used as the source of registries.
    val registryProvider = amqp.AmqpCloud(system, endPoint, registryRoutingInfo)

    new HttpCloud(
      routingInfo,
      system,
      registryProvider
    )
  }
}
