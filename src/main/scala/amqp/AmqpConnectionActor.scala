package codecraft.platform.amqp

import akka.actor._
import com.rabbitmq.client._
import codecraft.platform._
import scala.collection.mutable
import scala.util._
import codecraft.platform.amqp._
import codecraft.codegen._

private[amqp] class AmqpConnectionActor(
  conn: Connection,
  privateChan: Channel,
  privateQueueName: String,
  routingInfo: RoutingInfo
) extends Actor with ActorLogging {
  import AmqpConnectionActor._
  import AmqpCloudCommands._
  import AmqpCloudEvents._
  import AmqpChannelActor._
  import Control._

  val tasks = mutable.Map.empty[String, ConsumerTask]
  val channelActors = mutable.Map.empty[String, ActorRef]
  val privateConsumer = new DefaultConsumer(privateChan) {
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
  privateChan.basicConsume(
    privateQueueName,
    true,
    privateConsumer
  )

  setupRoutes(privateChan)

  def uuid = java.util.UUID.randomUUID.toString

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
      //throw exn
  }

  def setupRoutes(chan: Channel) {
    Try {
      // Cut through all of the commands and events.
      // Create the queues and exchanges.
      for ((_, info) <- routingInfo.group) {
        log.info(s"Declaring topic exchange ${info.exchange}")
        privateChan.exchangeDeclare(
          info.exchange,
          "topic",
          true,
          false,
          false,
          null
        )
        log.info(s"Declaring queue ${info.queueName}")
        privateChan.queueDeclare(
          info.queueName,
          true,
          false,
          false,
          null
        )
        log.info(s"Binding queue ${info.queueName} to ${info.exchange} => ${info.routingKey}")
        privateChan.queueBind(
          info.queueName,
          info.exchange,
          info.routingKey
        )
      }
      //for ((_, info) <- RoutingInfo.eventInfo) {
      //  privateChan.exchangeDeclare(
      //    info.exchange,
      //    "fanout",
      //    true,
      //    false,
      //    false,
      //    null
      //  )
      //}
    } recover handleFailure
  }

  def handleDelivery(event: Delivery) {
    log.info(s"Handling event $event")

    Try {
      val routingKey = Option(event.envelope.getRoutingKey).getOrElse {
        throw RoutingKeyUndefined
      }

      Option(event.props.getCorrelationId) match {
        case None =>
          log.warning(s"Received delivery from routingKey $routingKey with empty correlationId -- Msg not deserialized.")

        case Some(taskId) =>
          tasks.get(taskId) match {
            case None =>
              log.warning(s"Received delivery from routingKey $routingKey with unmapped correlationId -- Msg not deserialized.")

            case Some(task) =>
              log.info(s"Resolving task $task")

              // Valid id, valid task. Deserialize this message and respond.
              Control.deserializeLetter(event.body) match {
                case Failure(e) =>
                  log.warning(s"Task $task is failed with $e")
                  task.sender ! Failure(e)

                case Success(state) =>
                  log.info(s"Task $task is resolved with $state")

                  state match {
                    case BackoffState(millis) =>
                      // We need to backoff here and attempt to republish in the future.
                      // TODO: implement.
                      val e = new Exception("Cannot handle backoff at this time.")
                      task.sender ! Failure(e)
                      throw e

                    case FailedState(event) =>
                      Failure(new Exception(event))
                      task.sender ! Failure(new Exception(event))

                    case SuccessState(payload) =>
                      // Deserialize the payload.
                      val reply = task.info.deserializeReply(payload)
                      // Publish the reply back to the sender.
                      task.sender ! Success(reply)
                  }
              }
          }
      }
    } recover handleFailure
  }

  def handleRequestCmd(
    msg: RequestCmd
  ) {
    Try {
      log.info(s"Looking routing info for ${msg.cmdKey}")
      val info = routingInfo.cmd(msg.cmdKey)
      log.info(s"Got routing info for ${msg.cmdKey}")
      val data = info.serializeCmd(msg.cmd).get
      val id = uuid

      log.info(s"Publishing request to queue ${info.key}")

      val props = new AMQP.BasicProperties
        .Builder()
        .correlationId(id)
        .replyTo(privateQueueName)
        .build()

      privateChan.basicPublish(
        info.group.exchange,
        //info.group.routingKey,
        info.key,
        props,
        data
      )

      tasks += ((id, ConsumerTask(id, info, sender)))
    } recover handleFailure
  }

  def handleSubscribeCmd(msg: SubscribeCmd) {
    Try {
      log.info(s"Handling SubscribeCmd $msg")

      val groupInfo = routingInfo group msg.groupKey

      val chanActor = channelActors.getOrElseUpdate(groupInfo.routingKey, {
        val chan = conn.createChannel
        val props = AmqpChannelActor.props(chan, routingInfo)
        val child = context.actorOf(props)

        // Supervise the channel actor.
        context watch child

        child
      })

      chanActor.tell(msg, sender)
    } recover handleFailure
  }

  def subscribeEvent(msg: SubscribeEvent) {
    Try {
      log.info(s"Handling SubscribeEvent $msg")

      val info = routingInfo event msg.eventKey
      val chanActor = channelActors.getOrElseUpdate(info.key, {
        val chan = conn.createChannel
        val props = AmqpChannelActor.props(chan, routingInfo)
        val child = context.actorOf(props)

        context watch child

        child
      })

      chanActor.tell(msg, sender)
    } recover handleFailure
  }

  def publishEvent(msg: PublishEvent) {
    Try {
      // Get the routing info and serialize this event.
      val info = routingInfo event (msg.eventKey)
      val data = info serialize (msg.event) get

      // Publish this event.
      val props = new AMQP.BasicProperties
        .Builder()
        .build()
      privateChan.basicPublish(
        info exchange,
        info key,
        props,
        data
      )
    } recover handleFailure
  }

  def receive = {
    case msg: PublishEvent => publishEvent(msg)
    case msg: SubscribeEvent => subscribeEvent(msg)
    case msg: RequestCmd => handleRequestCmd(msg)
    case msg: SubscribeCmd => handleSubscribeCmd(msg)
    case event: Delivery => handleDelivery(event)
    case ShutdownCmd =>
      // Shutdown this actor and it's children.
      log.info(s"Closing connection to RabbitMQ")
      Try {
        context stop self
        conn.close()
      }
      sender ! Success()
  }
}

object AmqpConnectionActor {
  case class ConsumerTask(
    id: String,
    info: CmdRegistry,
    sender: ActorRef
  )

  def props(
    conn: Connection,
    privateChan: Channel,
    privateQueueName: String,
    routingInfo: RoutingInfo
  ) = Props(
    new AmqpConnectionActor(
      conn,
      privateChan,
      privateQueueName,
      routingInfo
    )
  )
}

