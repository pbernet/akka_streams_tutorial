
import io.gatling.core.Predef.*
import io.gatling.http.Predef.*

import scala.concurrent.duration.*

/**
  * Start [[akkahttp.ReverseProxy]]
  * Run this simulation from terminal:
  * sbt 'Gatling/testOnly ReverseProxySimulation'
  * or from sbt shell:
  * Gatling/testOnly ReverseProxySimulation
  */
class ReverseProxySimulation extends Simulation {
  val baseUrl = "http://127.0.0.1:8080"

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .userAgentHeader("Gatling")

  val scn = scenario("GatlingLocalClient")
    .exec(session => session.set("correlationId", 1))
    .repeat(10) {
      exec(
        http("Local Mode Request")
          .get("/")
          .header("Host", "local")
          .header("X-Correlation-ID", session => s"load-${session.userId}-${session("correlationId").as[Int]}")
          .check(status.is(200))
          .check(status.saveAs("responseStatus"))
          .check(header("X-Correlation-ID": CharSequence).saveAs("responseCorrelationId"))
      )
        .exec(session => {
          println(s"Got: ${session.status} response with HTTP status: ${session("responseStatus").as[String]} for id: ${session("responseCorrelationId").as[String]}")
          session
        })
        .exec(session => session.set("correlationId", session("correlationId").as[Int] + 1))
    }

  // Adjust loadFactor to scale load per peak
  def peak(name: String, loadFactor: Double) =
    scenario(name)
      .exec(scn)
      .inject(
        nothingFor(5.seconds), // initial quiet period
        rampUsers((20 * loadFactor).toInt).during(10.seconds), // ramp up
        constantUsersPerSec(50 * loadFactor).during(20.seconds), // peak load
        rampUsersPerSec(50 * loadFactor).to(10 * loadFactor).during(10.seconds), // ramp down
        constantUsersPerSec(10 * loadFactor).during(10.seconds), // tail off
        nothingFor(30.seconds) // cool down period
      )

  setUp(
    peak("Morning Peak", 0.01)
      .andThen(peak("Midday Peak", 0.02))
      .andThen(peak("Evening Peak", 0.03))
  ).protocols(httpProtocol)
}
