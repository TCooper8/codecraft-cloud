package codecraft.platform.amqp

import akka.actor._
import akka.actor.ActorDSL._
import akka.event.LoggingAdapter
import akka.event.Logging
import java.net.URI
import akka.pattern.ask
import akka.util.Timeout
import com.rabbitmq.client._
import codecraft.platform._
import play.api.libs.json._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.util._

private[amqp] class AmqpCloudActor(
  routingInfo: RoutingInfo
) extends Actor with ActorLogging {
  import AmqpCloudCommands.Connect

  val factory = new ConnectionFactory()

  def receive = {
    case Connect(endPoints) =>
      Try {
        // Parse the end points to RabbitMQ addresses.
        log.info(s"Parsing address list from $endPoints")
        val addresses = endPoints.map {
          case endPoint =>
            val uri = new URI(endPoint)
            new com.rabbitmq.client.Address(uri.getHost, uri.getPort)
        } toArray

        log.info(s"Connecting to RabbitMQ with $addresses")
        val conn = factory.newConnection(addresses)
        val chan = conn.createChannel()
        val privateQueueName = chan.queueDeclare.getQueue

        val connActor = context.actorOf(AmqpConnectionActor.props(
          conn,
          chan,
          privateQueueName,
          routingInfo
        ))

        connActor
      } match {
        case result =>
          // Send back the Try[ActorRef]
          sender ! result
      }
  }
}

private[amqp] object AmqpCloudActor {
  def props(routingInfo: RoutingInfo) = Props(
    new AmqpCloudActor(
      routingInfo
    )
  )
}
