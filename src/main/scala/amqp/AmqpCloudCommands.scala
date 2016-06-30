package codecraft.platform.amqp

import akka.actor.ActorRef
import scala.util.Try
import codecraft.platform._
import codecraft.codegen._

private[amqp] object AmqpCloudCommands {
  sealed trait Cmd
  sealed trait Reply

  // Connecting
  final case class Connect(
    endPoints: List[String]
  ) extends Cmd

  final case class RequestCmd(
    cmdKey: String,
    cmd: Any
  ) extends Cmd

  final case class RequestCmdReply(
    result: Try[Any]
  ) extends Reply

  //final case class SubscribeCmd(
  //  cmdKey: String,
  //  actor: ActorRef
  //)

  final case class SubscribeCmd(
    groupKey: String,
    service: CmdGroupConsumer
  )

  final case class SubscribeCmdReply(
    result: Try[Any]
  ) extends Reply

  final case object ShutdownCmd extends Cmd
}

