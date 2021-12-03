package akkahttp.oidc

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akkahttp.oidc.UserRegistryActor.ActionPerformed
import spray.json.DefaultJsonProtocol._

trait JsonSupport extends SprayJsonSupport {

  case class Keys(keys: Seq[KeyData])

  case class KeyData(kid: String, n: String, e: String)

  implicit val keyDataFormat = jsonFormat3(KeyData)
  implicit val keysFormat = jsonFormat1(Keys)


  implicit val userJsonFormat = jsonFormat3(User)
  implicit val usersJsonFormat = jsonFormat1(Users)

  implicit val actionPerformedJsonFormat = jsonFormat1(ActionPerformed)
}