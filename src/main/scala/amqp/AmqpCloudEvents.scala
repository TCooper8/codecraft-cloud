package codecraft.platform.amqp

import akka.actor.ActorRef
import com.rabbitmq.client._
import codecraft.codegen._

private[amqp] object AmqpCloudEvents {
  sealed trait Event

  final case class Delivery(
    consumerTag: String,
    envelope: Envelope,
    props: AMQP.BasicProperties,
    body: Array[Byte]
  ) extends Event

  final case class DeliveryFailure(
    deliveryTag: Long,
    correlationId: String,
    replyTo: String,
    sub: Option[CmdGroupConsumer],
    exn: Throwable
  )

  final case class DeliverySuccess(
    deliveryTag: Long,
    correlationId: String,
    replyTo: String,
    replyData: Array[Byte]
  )

  final case class PrivateDelivery(
    consumerTag: String,
    envelope: Envelope,
    props: AMQP.BasicProperties,
    body: Array[Byte]
  ) extends Event
}

