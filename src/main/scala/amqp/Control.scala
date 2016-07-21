package codecraft.platform.amqp

import scala.util.Try
import play.api.libs.json._

private[amqp] object Control {
  sealed trait IState
  final case class BackoffState(
    millis: Int
  )
  final case class FailedState(
    message: String
  )
  final case class SuccessState(
    payload: Array[Byte]
  )

  final case class Letter(
    caseType: String,
    statePayload: Array[Byte]
  )

  val backoffCaseType = "backoff"
  val failedCaseType = "failed"
  val successCaseType = "success"

  private[this] implicit val backoffFormat = Json.format[BackoffState]
  private[this] implicit val failedFormat = Json.format[FailedState]
  private[this] implicit val successFormat = Json.format[SuccessState]
  private[this] implicit val format = Json.format[Letter]

  def backoff(millis: Int) = Letter(
    backoffCaseType,
    Json.toJson(BackoffState(millis)).toString.getBytes
  )

  def failed(message: String) = Letter(
    failedCaseType,
    Json.toJson(FailedState(message)).toString.getBytes
  )

  def success(payload: Array[Byte]) = Letter(
    successCaseType,
    Json.toJson(SuccessState(payload)).toString.getBytes
  )

  def serializeLetter(letter: Letter) = Try {
    val json = Json.toJson(letter)(format)
    json.toString.getBytes
  }

  def deserializeLetter(data: Array[Byte]) = Try {
    val json = Json.parse(new String(data))

    println(s"Parsed json $json from data")

    Json.fromJson[Letter](json)(format) match {
      case JsError(e) =>
        println(s"Json is invalid with $e")
        throw new Exception(e mkString)

      case JsSuccess(letter, _) =>
        val caseJson = Json.parse(new String(letter.statePayload))

        letter.caseType match {
          case "backoff" =>
            Json.fromJson[BackoffState](caseJson) match {
              case JsError(e) => throw new Exception(e mkString)
              case JsSuccess(state, _) => state
            }
          case "failed" =>
            Json.fromJson[FailedState](caseJson) match {
              case JsError(e) => throw new Exception(e mkString)
              case JsSuccess(state, _) => state
            }
          case "success" =>
            println(s"Success json = $caseJson")
            Json.fromJson[SuccessState](caseJson) match {
              case JsError(e) => throw new Exception(e mkString)
              case JsSuccess(state, _) => state
            }
        }
    }
  }
}


