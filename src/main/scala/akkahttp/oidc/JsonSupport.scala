package akkahttp.oidc

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol._

trait JsonSupport extends SprayJsonSupport {

  case class Keys(keys: Seq[KeyData])
  case class KeyData(kid: String, n: String, e: String)

  implicit val keyDataFormat = jsonFormat3(KeyData)
  implicit val keysFormat = jsonFormat1(Keys)

  final case class UserKeycloak(firstName: Option[String], lastName: Option[String], email: Option[String])
  final case class UsersKeycloak(users: Seq[UserKeycloak])

  implicit val userJsonFormat = jsonFormat3(UserKeycloak)
  implicit val usersJsonFormat = jsonFormat1(UsersKeycloak)
}