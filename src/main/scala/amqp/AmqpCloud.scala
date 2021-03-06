package codecraft.platform.amqp

import akka.actor._
import akka.pattern.ask
import codecraft.platform._
import scala.util._
import akka.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import codecraft.codegen._

private[amqp] class AmqpCloud(
  routingInfo: RoutingInfo,
  system: ActorSystem,
  cloudActor: ActorRef
) extends ICloud {
  import AmqpCloudCommands._

  implicit val ex = system.dispatcher

  def publishEvent(eventKey: String, event: Any, timeoutDur: FiniteDuration = 5 seconds): Future[Unit] = Future {
    throw new Exception("Not implemented")
  }

  def requestCmd(cmdKey: String, cmd: Any, timeoutDur: FiniteDuration = 5 seconds): Future[Any] = Future {
    val timeout = Timeout(timeoutDur)
    val task = (cloudActor ? RequestCmd(cmdKey, cmd))(timeout).map {
      case Failure(e) => throw e
      case Success(result) =>
        result match {
          case Failure(e) => throw e
          //case Success(Failure(e)) => throw e
          case Success(reply) => reply
        }
    }

    Await.result(task, timeoutDur)
  }

  def subscribeCmd(cmdGroupKey: String, service: CmdGroupConsumer, timeoutDur: FiniteDuration): Future[Unit] = Future {
    println(s"Subscribing ${service.id} to $cmdGroupKey")
    // Verify that the group key is valid.
    routingInfo group cmdGroupKey

    val timeout = Timeout(timeoutDur)
    //val task = (cloudActor ? SubscribeCmd(s"cmd.$cmdKey", actor))(timeout)
    val task = (cloudActor ? SubscribeCmd(cmdGroupKey, service))(timeout)

    Await.result(task, timeoutDur)
  }

  def publishEvent(eventKey: String, event: Any): Future[Unit] = Future {
    println(s"Publishing event $eventKey => $event")
    // Verify that the event key is valid.
    routingInfo event eventKey

    val timeout = Timeout(180 hours)
    val task = (cloudActor ? PublishEvent(eventKey, event))(timeout) flatMap {
      case Success(_) => Future{ () }
      case Failure(e) => Future.failed(e)
    }
    Await.result(task, timeout.duration)
  }

  def subscribeEvent(eventKey: String, method: Any => Unit): Future[Unit] = Future {
    // Verify that the event key is valid.
    routingInfo event eventKey
    val timeout = Timeout(180 hours)
    (cloudActor ? SubscribeEvent(eventKey, method))(timeout) flatMap {
      case Failure(e) => Future failed e
      case _ => Future{ () }
    }
  }

  def disconnect() {
    Await.result((cloudActor ? ShutdownCmd)(Timeout(5 seconds)), 5 seconds) match {
      case Success(_) =>
        println(s"Connection closed.")
    }
  }
}

object AmqpCloud {
  import AmqpCloudCommands.Connect

  def apply(system: ActorSystem, endPoint: String, routingInfo: RoutingInfo): ICloud = {
    val cloudActor = system.actorOf(
      AmqpCloudActor.props(routingInfo)
    )
    implicit val ex = system.dispatcher
    val timeout = Timeout(10 seconds)
    val task = (cloudActor ? Connect(endPoint))(timeout).mapTo[Try[ActorRef]]
    val conn = Await.result(task, timeout.duration).get

    println(s"Created cloud with $routingInfo")

    (new AmqpCloud(routingInfo, system, conn)).asInstanceOf[ICloud]
  }
}
