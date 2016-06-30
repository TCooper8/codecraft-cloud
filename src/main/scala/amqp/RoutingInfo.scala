//package codecraft.platform.amqp.routing
//
//import play.api.libs.json._
//import scala.util.Try
//import codecraft.platform._
//
//final case class CmdInfoNotFound(cmdKey: String) extends Exception(s"CmdInfo $cmdKey not found")
//final case class EventInfoNotFound(cmdKey: String) extends Exception(s"EventInfo $cmdKey not found")
//final case class InvalidCmdGroupKey(key: CmdGroupKey) extends Exception(s"$key is an unmapped CmdGroupKey")
//
//final case class CmdRegistry(
//  key: String,
//  serializeCmd: Any => Try[Array[Byte]],
//  serializeReply: Any => Try[Array[Byte]],
//  deserializeCmd: Array[Byte] => Try[Any],
//  deserializeReply: Array[Byte] => Try[Any]
//)
//
//final case class GroupRouting(
//  queueName: String,
//  routingKey: String,
//  exchange: String
//)
//
//case class CmdRouting(
//  routingKey: String,
//  exchange: String,
//  registry: CmdRegistry,
//  group: GroupRouting
//)
//
//trait RoutingGroup {
//  val cmdInfo: List[CmdRegistry]
//}
//
//object Utils {
//  def jsonSerialize[A](format: Format[A]) = {
//    (any: Any) => Try {
//      val value = any.asInstanceOf[A]
//      Json.toJson(value)(format).toString.getBytes
//    }
//  }
//
//  def jsonDeserialize[A](format: Format[A]) = {
//    (bytes: Array[Byte]) => Try {
//      val json = Json.parse(new String(bytes))
//      Json.fromJson[A](json)(format) match {
//        case JsError(errors) =>
//          throw new Exception(errors mkString)
//        case JsSuccess(any, _) =>
//          any.asInstanceOf[A]
//      }
//    }
//  }
//
//  def mkJsonCmdRegistry[A, B](key: String)(implicit msgFormat: Format[A], replyFormat: Format[B]) = CmdRegistry(
//    key,
//    jsonSerialize(msgFormat),
//    jsonSerialize(replyFormat),
//    jsonDeserialize(msgFormat),
//    jsonDeserialize(replyFormat)
//  )
//}

/*
 * This class is used as a static lookup of routing information.
 * */
//object RoutingInfo {
//  private[this] lazy val groupInfo = Map[String, GroupRouting](
//    ("userstore", GroupRouting(
//      "cmd.userstore",
//      "cmd.userstore.*",
//      "cmd"
//    ))
//  )
//
//  /*
//   * Utility for generating a Map[String, CmdRouting] from a given RoutingGroup.
//   * */
//  private[this] def groupCmdInfo(groupKey: String, group: RoutingGroup) = group.cmdInfo.map {
//    case registry =>
//      val key = s"cmd.${groupKey toLowerCase}.${registry.key toLowerCase}"
//      val routing = CmdRouting(
//        key,
//        "cmd",
//        registry,
//        groupInfo(groupKey)
//      )
//      (key, routing)
//  }.toMap
//
//  lazy val cmd =
//    Map.empty[String, CmdRouting] ++ groupCmdInfo("userstore", userstore.userstoreRoutingGroup)
//
//  lazy val groups = groupInfo.map{ case (_, value) => value }.toList
//
//  /*
//   * Used to lookup a group via a staticly defined key, instead of a generated string.
//   * */
//  def group(groupKey: CmdGroupKey) = groupKey match {
//    case UserStoreCmdGroupKey => groupInfo("userstore")
//    case _ => throw InvalidCmdGroupKey(groupKey)
//  }
//}
