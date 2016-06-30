package codecraft.platform.amqp

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import com.rabbitmq.client._
import codecraft.platform._
import codecraft.platform._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util._
import codecraft.platform.amqp._
import codecraft.platform._
import scala.concurrent.Future
import codecraft.codegen.CmdGroupConsumer

private[amqp] class AmqpChannelActor(
  chan: Channel,
  routingInfo: RoutingInfo
) extends Actor with ActorLogging {
  import AmqpChannelActor._
  import AmqpCloudCommands.{SubscribeCmd, ShutdownCmd}
  import AmqpCloudEvents.{Delivery, DeliveryFailure, DeliverySuccess}

  import context._

  val cmdSubscriptions = mutable.Map.empty[String, List[CmdGroupConsumer]]
  val cmdSubscriptionServiceIndex = mutable.Map.empty[String, String]

  log.info(s"Started channel $self")

  def handleFailure: PartialFunction[Throwable, Unit] = {
    case exn: CmdInfoNotFound =>
      // Non-volatile exception.
      sender ! Failure(exn)

    case exn: EventInfoNotFound =>
      // Non-volatile exception.
      sender ! Failure(exn)

    case exn =>
      // Volatile
      sender ! Failure(exn)
      throw exn
  }

  def handleSubscribeCmd(cmd: SubscribeCmd) = {
    //Try { RoutingInfo.cmdInfo(cmd.cmdKey) } match {
    Try { routingInfo.group(cmd.groupKey) } match {
      case Failure(e) =>
        // Unable to get info, send back a failure response.
        sender ! Failure(e)

      case Success(info) =>
        // If any of this fails, there is a connection issue.
        Try {
          val consumer = new DefaultConsumer(chan) {
            override def handleDelivery(
              consumerTag: String,
              envelope: Envelope,
              props: AMQP.BasicProperties,
              body: Array[Byte]
            ) = {
              self ! Delivery(
                consumerTag,
                envelope,
                props,
                body
              )
            }
          }

          chan.basicConsume(
            info.queueName,
            false,
            consumer
          )
          log.info(s"Started consumer for ${cmd.groupKey}-$sender")

          // Watch the child for termination.
          //context watch cmd.actor

          // Add the actor to the subscriptions.
          val subs = cmdSubscriptions.getOrElse(info.queueName, List.empty)
          cmdSubscriptions += ((info.queueName, cmd.service::subs))
          cmdSubscriptionServiceIndex += ((cmd.service.id, info.queueName))
          //val subs = cmdSubscriptions.getOrElse(info.routingKey, List.empty)
          //cmdSubscriptions += ((info.routingKey, cmd.service::subs))

          ()
        } match {
          case Failure(e) =>
            sender ! Failure(e)
            throw e

          case Success(reply) =>
            sender ! Success(reply)
        }
    }
  }

  def handleDelivery(event: Delivery) = {
    log.info(s"Handling delivery $event")
    val envelope = event.envelope
    val props = event.props

    val cmdKey = Option(envelope.getRoutingKey).getOrElse {
      throw RoutingKeyUndefined
    }
    log.info(s"Pulling routing info from $cmdKey")
    val info = routingInfo.cmd(cmdKey)
    //val registry = info.registry
    val group = info.group

    log.info(s"Pulling subscribers for $group")

    // Check and validate the properties needed to handle a reply.
    val replyTo = Option(props.getReplyTo).getOrElse {
      throw ReplyToUndefined
    }
    val correlationId = Option(props.getCorrelationId).getOrElse {
      throw CorrelationIdUndefined
    }

    cmdSubscriptions.get(group.queueName) match {
      case None =>
        self ! DeliveryFailure(envelope.getDeliveryTag, correlationId, replyTo, None, new Exception("No subscribers found for this command -- Nack not implemented."))
      case Some(Nil) =>
        self ! DeliveryFailure(envelope.getDeliveryTag, correlationId, replyTo, None, new Exception("No subscribers found for this command -- Nack not implemented."))
      case Some(subs) =>
        val sub = subs((util.Random.nextFloat * subs.length).toInt)
        val timeout = Timeout(5 seconds)

        // Deserialize the command.
        val cmd = info.deserializeCmd(event.body).get
        // Allocate a new task to resolve the future of this command.
        log.info(s"Requesting $cmd from $sub")

        Future {
          log.info(s"Pulling method from registry under ${info.key}")
          val method = sub.methodRegistry(info.key)
          val reply = method(cmd)
          val replyData = info.serializeReply(reply).get

          (reply, replyData)
          } onComplete {
            case Failure(e) =>
              log.warning(s"Sub $sub failed from command $cmd")
              Try { sub.onError(e) }
              self ! DeliveryFailure(envelope.getDeliveryTag, correlationId, replyTo, Some(sub), e)

            case Success((reply, replyData)) =>
              log.info(s"Sub $sub responded to $cmd with $reply")
              self ! DeliverySuccess(envelope.getDeliveryTag, correlationId, replyTo, replyData)
          }
    }
  }

  def publishResponse(
    deliveryTag: Long,
    replyTo: String,
    correlationId: String,
    payload: Array[Byte]
  ) {
    val props = new AMQP.BasicProperties
      .Builder()
      .correlationId(correlationId)
      .build()

    chan.basicPublish(
      "",
      replyTo,
      props,
      payload
    )

    chan.basicAck(
      deliveryTag,
      false
    )
  }

  def handleDeliveryFailure(event: DeliveryFailure) {
    val exn = event.exn
    log.info(s"Handling event $event and error ${exn.printStackTrace}")

    Try {
      // Kill the subscription -- The Terminated event will actually remove the subscription.
      // Shutdown this subscription.
      event.sub.foreach { sub =>
        cmdSubscriptionServiceIndex.get(sub.id).map {
          case queueName =>
            cmdSubscriptions.get(queueName).map {
              case Nil =>
                cmdSubscriptions -= queueName
              case subs =>
                cmdSubscriptions += ((queueName, subs.filter(_.id != sub.id)))
            }
        }
      }

      // Form a failed letter and respond to the queue with it.
      val payload = Control.serializeLetter(Control.failed(exn.getMessage)).get

      publishResponse(
        event.deliveryTag,
        event.replyTo,
        event.correlationId,
        payload
      )
    }.recover(handleFailure)
  }

  def handleDeliverySuccess(event: DeliverySuccess) {
    log.info(s"Handling event $event")

    Try {
      log.info(s"Publishing response from successful delivery to ${event.replyTo}")
      val payload = Control.serializeLetter(Control.success(event.replyData)).get

      publishResponse(
        event.deliveryTag,
        event.replyTo,
        event.correlationId,
        payload
      )
    }.recover(handleFailure)
  }

  def receiveCloudCommands: Receive = {
    case cmd: SubscribeCmd => handleSubscribeCmd(cmd)
  }

  def receiveCloudEvents: Receive = {
    case event: Delivery =>
      Try { handleDelivery(event) } match {
        case Failure(e) =>
          log.warning(s"Error handling delivery for $event: $e")

        case Success(_) => ()
      }
    case event: DeliveryFailure => handleDeliveryFailure(event)
    case event: DeliverySuccess => handleDeliverySuccess(event)
  }

  def receiveWatch: Receive = {
    //case Terminated(child) =>
    //  cmdSubscriptionServiceIndex.get(child).map {
    //    case cmdKey =>
    //      // Remove the index.
    //      cmdSubscriptionServiceIndex -= child
    //      // Remove the subscription.
    //      cmdSubscriptions.get(cmdKey).map {
    //        case Nil =>
    //          cmdSubscriptions -= cmdKey
    //        case subs =>
    //          // Filter the child from the subscriptions.
    //          cmdSubscriptions += ((cmdKey, subs.filter(_ != child)))
    //      }
    //  }

    case ShutdownCmd =>
      // Shutdown this channel.
      chan.close()
      context stop self
  }

  def receive =
    receiveCloudCommands orElse receiveCloudEvents orElse receiveWatch
}

private[amqp] object AmqpChannelActor {
  case object RoutingKeyUndefined extends Exception("Routing key is undefined")
  case object CorrelationIdUndefined extends Exception("CorrelationId is undefined")
  case object ReplyToUndefined extends Exception("ReplyTo is undefined")
  case object UnmappedDelivery

  def props(chan: Channel, routingInfo: RoutingInfo) = Props(
    new AmqpChannelActor(
      chan,
      routingInfo
    )
  )
}

