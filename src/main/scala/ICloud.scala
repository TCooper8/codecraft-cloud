package codecraft.platform

import scala.concurrent.Future
import scala.concurrent.duration._
import codecraft.codegen.CmdGroupConsumer

final case class CmdInfoNotFound(cmdKey: String) extends Exception(s"CmdInfo $cmdKey not found")
final case class EventInfoNotFound(cmdKey: String) extends Exception(s"EventInfo $cmdKey not found")
final case class InvalidCmdGroupKey(key: CmdGroupKey) extends Exception(s"$key is an unmapped CmdGroupKey")

sealed trait CmdGroupKey
final case object UserStoreCmdGroupKey extends CmdGroupKey

trait ICloud {
  def requestCmd(cmdKey: String, cmd: Any, timeoutDur: FiniteDuration): Future[Any]
  def subscribeCmd(groupKey: String, service: CmdGroupConsumer, timeoutDur: FiniteDuration = 5 seconds): Future[Unit]
  //def publishEvent(eventKey: String, event: Any, timeoutDur: FiniteDuration): Future[Unit]
  //def subscribeEvent(eventKey: String, actor: ActorRef, timeoutDur: FiniteDuration): Future[Unit]
  def disconnect(): Unit
}
