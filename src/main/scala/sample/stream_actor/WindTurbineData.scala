package sample.stream_actor

import scala.util.Random

case class MeasurementsContainer(id: String = "", timestamp: Long = 0, measurements: Measurements = Measurements())

case class Measurements(power: Double = 0, rotor_speed : Double = 0, wind_speed: Double = 0)

object WindTurbineData {
  def apply(id: String) = new WindTurbineData(id)
}

class WindTurbineData(id: String) {
  val random = Random

  def getNext: String = {
    val timestamp = System.currentTimeMillis / 1000
    val power = f"${random.nextDouble() * 10}%.2f"
    val rotorSpeed = f"${random.nextDouble() * 10}%.2f"
    val windSpeed = f"${random.nextDouble() * 100}%.2f"

    s"""{
       |    "id": "$id",
       |    "timestamp": $timestamp,
       |    "measurements": {
       |        "power": $power,
       |        "rotor_speed": $rotorSpeed,
       |        "wind_speed": $windSpeed
       |    }
       |}""".stripMargin
  }
}
