package alpakka.tcp_to_websockets.hl7mllp

/**
  * Doc MLLP:
  * http://hl7.ihelse.net/hl7v3/infrastructure/transport/transport_mllp.html
  * and [[ca.uhn.hl7v2.llp.MllpConstants]]
  */
trait MllpProtocol {

  //MLLP messages begin after hex "0x0B" and continue until "0x1C|0x0D"
  val START_OF_BLOCK = "\u000b" //0x0B
  val END_OF_BLOCK = "\u001c" //0x1C
  val CARRIAGE_RETURN = "\r" //0x0D

  def encodeMllp(message: String): String = {
    START_OF_BLOCK + message + END_OF_BLOCK + CARRIAGE_RETURN
  }

  // The HAPI parser needs /r as segment terminator, but this is not printable
  def printable(message: String): String = {
    message.replace("\r", "\n")
  }

  def printableShort(message: String): String = {
    printable(message).take(20).concat("...")
  }
}
