//package codecraft.platform.amqp.routing.userstore
//
//import play.api.libs.json._
//import codecraft.messages.userstore._
//import codecraft.platform.amqp.routing.{Utils, RoutingGroup}
//import codecraft.messages.userstore.CreateUser
//import codecraft.messages.userstore.CreateUserReply
//import codecraft.messages.userstore.UserPlans
//import codecraft.messages.userstore.UpdateUser
//import codecraft.messages.userstore.UpdateUserReply
//import codecraft.messages.userstore.GetUser
//import codecraft.messages.userstore.GetUserReply
//
//private[routing] object userstoreFormatters {
//  implicit val UserPlansFormat = Json.format[UserPlans]
//  implicit val UserFormat = Json.format[User]
//  implicit val UpdateUserFormat = Json.format[UpdateUser]
//  implicit val UpdateUserReplyFormat = Json.format[UpdateUserReply]
//  implicit val CreateUserFormat = Json.format[CreateUser]
//  implicit val CreateUserReplyFormat = Json.format[CreateUserReply]
//  implicit val GetUserFormat = Json.format[GetUser]
//  implicit val GetUserReplyFormat = Json.format[GetUserReply]
//}
//
//private[routing] object userstoreRoutingGroup extends RoutingGroup {
//  import userstoreFormatters._
//  lazy val cmdInfo = List(
//    Utils.mkJsonCmdRegistry[CreateUser, CreateUserReply]("createUser"),
//    Utils.mkJsonCmdRegistry[String, UserPlans]("getUserPlans"),
//    Utils.mkJsonCmdRegistry[UpdateUser, UpdateUserReply]("updateUser"),
//    Utils.mkJsonCmdRegistry[GetUser, GetUserReply]("getUser")
//  )
//}
