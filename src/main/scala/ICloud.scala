package codecraft.platform

import akka.actor.ActorSystem
import scala.concurrent.Future
import scala.concurrent.duration._
import codecraft.codegen._

final case class CmdInfoNotFound(cmdKey: String) extends Exception(s"CmdInfo $cmdKey not found")
final case class EventInfoNotFound(cmdKey: String) extends Exception(s"EventInfo $cmdKey not found")
final case class InvalidCmdGroupKey(key: CmdGroupKey) extends Exception(s"$key is an unmapped CmdGroupKey")

sealed trait CmdGroupKey
final case object UserStoreCmdGroupKey extends CmdGroupKey

final case class RoutingInfo(
  val cmd: Map[String, CmdRegistry],
  val event: Map[String, EventRegistry],
  val group: Map[String, GroupRouting]
)

trait ICloud {
  def requestCmd(cmdKey: String, cmd: Any, timeoutDur: FiniteDuration): Future[Any]
  def subscribeCmd(groupKey: String, service: CmdGroupConsumer, timeoutDur: FiniteDuration = 5 seconds): Future[Unit]
  def publishEvent(eventKey: String, event: Any): Future[Unit]
  def subscribeEvent(eventKey: String, method: Any => Unit): Future[Unit]
  def disconnect(): Unit
}

private[platform] case class HybridCloud(
  system: ActorSystem,
  requestCmdAction: Any => Boolean,
  clouds: List[ICloud]
) extends ICloud {
  import system.dispatcher

  def requestCmd(cmdKey: String, cmd: Any, timeoutDur: FiniteDuration): Future[Any] = {
    def loop(clouds: List[ICloud], errorAcc: List[Throwable]): Future[Any] = clouds match {
      case Nil =>
        Future failed (new Exception(errorAcc mkString ", "))
      case cloud::clouds =>
        cloud requestCmd (cmdKey, cmd, timeoutDur) map { result =>
          if (requestCmdAction(result)) result
          else loop(clouds, errorAcc)
        } recover {
          case e => loop(clouds, e::errorAcc)
        }
    }

    loop(clouds, Nil)
  }

  def subscribeCmd(groupKey: String, service: CmdGroupConsumer, timeoutDur: FiniteDuration = 5 seconds): Future[Unit] = {
    val tasks = clouds map { cloud =>
      cloud subscribeCmd (groupKey, service, timeoutDur)
    }
    Future sequence tasks map { _ => () }
  }

  def publishEvent(eventKey: String, event: Any): Future[Unit] = {
    val tasks = clouds map { cloud =>
      cloud publishEvent (eventKey, event)
    }
    Future sequence tasks map { _ => () }
  }

  def subscribeEvent(eventKey: String, method: Any => Unit): Future[Unit] = {
    val tasks = clouds map { cloud =>
      cloud subscribeEvent (eventKey, method)
    }
    Future sequence tasks map { _ => () }
  }

  def disconnect(): Unit = {
    clouds map { cloud =>
      cloud disconnect
    }
  }
}

object ICloud {
  def sequence(system: ActorSystem, requestCmdAction: Any => Boolean, clouds: List[ICloud]): ICloud = {
    HybridCloud(
      system,
      requestCmdAction,
      clouds
    )
  }
}
