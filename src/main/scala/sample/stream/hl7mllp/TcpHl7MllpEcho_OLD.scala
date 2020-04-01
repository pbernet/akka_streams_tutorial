package sample.stream.hl7mllp

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Keep, Sink, Source, Tcp}
import akka.util.ByteString
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * HL7/MLLP echo flow
  * https://doc.akka.io/docs/akka/current/stream/stream-io.html?language=scala
  *
  *
  *
  *
  *
  * Use without parameters to start server and 10 parallel clients.
  *
  * Use parameters `server 0.0.0.0 6001` to start server listening on port 6001.
  *
  * Use parameters `client 127.0.0.1 6001` to start client connecting to
  * server on 127.0.0.1:6001.
  *
  * cmd line client:
  * echo -n "Hello \u001c World" | nc 127.0.0.1 6160
  *
  * HapiSendMultipleMessagesExample official client:
  * sends END_OF_BLOCK
  * works with hardcoded Message Control ID
  *
  */
object TcpHl7MllpEcho_OLD extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val systemServer = ActorSystem("TcpHl7MllpEchoServer")
  val systemClient = ActorSystem("TcpHl7MllpEchoClient")

  val START_OF_BLOCK = '\u000b' //0x0B
  val END_OF_BLOCK =  '\u001c'  //0x1C
  val CARRIAGE_RETURN = '\r'    //0x0D
  val NEW_LINE = '\n'
  val END_OF_TRANSMISSION = -1  //TODO needed?


  //TODO Poc in separate class. When to stop END_OF_TRANSMISSION?
  case class Hl7Message(payload: String)

  val splitterFlow = Flow[ByteString].statefulMapConcat[Hl7Message] { () =>
    var buffer = ByteString.empty
    in => {
      val (found, remaining) = parseThings(buffer ++ in)
      buffer = remaining
      found
    }
  }

  //finds as many elements as it can from the available bytes, returning the found elements and the unused bytestring.
  def parseThings(bytes: ByteString): (Vector[Hl7Message], ByteString) = {

    if(bytes.toIterator.next().equals(END_OF_BLOCK.toByte))
      {
        (Vector(Hl7Message(bytes.toString())), bytes)
      } else
        (Vector(), bytes)
     }



  var serverBinding: Future[Tcp.ServerBinding] = _

    if (args.isEmpty) {
      val (address, port) = ("127.0.0.1", 6160)
      serverBinding = server(systemServer, address, port)
      (1 to 1).par.foreach(each => client(each, systemClient, address, port))
    } else {
      val (address, port) =
        if (args.length == 3) (args(1), args(2).toInt)
        else ("127.0.0.1", 6000)
      if (args(0) == "server") {
        serverBinding = server(systemServer, address, port)
      } else if (args(0) == "client") {
        client(1, systemClient, address, port)
      }
    }


  def server(system: ActorSystem, address: String, port: Int) = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val handler = Sink.foreach[Tcp.IncomingConnection] { connection =>

      // Validate/Parse the hairy HL7 message beast
      val hl7Parser = Flow[String]
        .wireTap(each => logger.info("About to parse:" + each))

      val welcomeMsg = s"Welcome to: ${connection.localAddress}, you are: ${connection.remoteAddress}"
      val welcomeSource = Source.single(welcomeMsg)

      val serverEchoFlow = Flow[ByteString]
        //TODO Detects marker, BUT frames only *last* line before 1st marker
//        .via(Framing.delimiter( //chunk the inputs up to the MLLP end marker (in Byte)
//          ByteString(NEW_LINE),
//          maximumFrameLength = 20000,
//          allowTruncation = true))
        .map((each: ByteString) => each.utf8String)
        .filterNot(each => each.equals(NEW_LINE))
        .filterNot(each => each.equals(CARRIAGE_RETURN))
        //TODO WHY is each not printed here? log/wireTap same
        .log("serverlogger" , each => s"After chunking: $each")
        .via(hl7Parser)
        //.merge(welcomeSource) // merge the initial banner after parser
        // TODO Generate general matching ACK message: takes the getMessageControlID from the parsed incoming message
        .map(each =>  {
          val buf = new StringBuilder
          buf += START_OF_BLOCK
          buf ++= "MSH|^~\\&|||||||ACK||P|2.4"
          buf += CARRIAGE_RETURN
          buf ++= "MSA|AA|"
          buf ++= "NIST-IZ-007.00" //hardcoded
          buf += CARRIAGE_RETURN
          buf += END_OF_BLOCK
          buf += CARRIAGE_RETURN

          //TODO WHY is the ack message not printed here
          logger.info(s"In: $each Out: ${buf.toString}")
          buf.toString
        })
         //TODO WHY is each not printed here
        .wireTap(each => (s"After Ack: $each"))
        .map(ByteString(_))

        .watchTermination()((_, done) => done.onComplete {
        case Failure(err) =>
          logger.info(s"Server flow failed: $err")
        case _ => logger.info(s"Server flow terminated for client: ${connection.remoteAddress}")
      })
      connection.handleWith(serverEchoFlow)
    }
    
    val connections = Tcp().bind(interface = address, port = port)
    val binding = connections.watchTermination()(Keep.left).to(handler).run()

    binding.onComplete {
      case Success(b) =>
        logger.info("Server started, listening on: " + b.localAddress)
      case Failure(e) =>
        logger.info(s"Server could not bind to: $address:$port: ${e.getMessage}")
        system.terminate()
    }

    binding
  }

  def client(id: Int, system: ActorSystem, address: String, port: Int): Unit = {
    implicit val sys = system
    implicit val ec = system.dispatcher

    val connection: Flow[ByteString, ByteString, Future[Tcp.OutgoingConnection]] = Tcp().outgoingConnection(address, port)

    val buf = new StringBuilder
    buf += START_OF_BLOCK
    buf ++= "MSH|^~\\&|ZIS|1^AHospital|||199605141144||ADT^A01|NIST-IZ-007.00|P|2.4|||"
    buf += CARRIAGE_RETURN
    buf ++= "AL|NE|||8859/15|"
    buf += CARRIAGE_RETURN
    buf ++= "EVN|A01|20031104082400.0000+0100|20031104082400"
    buf += CARRIAGE_RETURN
    buf ++= "PID||\"\"|10||Vries^Danny^D.^^de||19951202|M|||Rembrandlaan^7^Leiden^^7301TH^\"\""
    buf ++= "^^P||\"\"|\"\"||\"\"|||||||\"\"|\"\"" +
    buf ++= "PV1||I|3w^301^\"\"^01|S|||100^van den Berg^^A.S."
    buf ++= "^^\"\"^dr|\"\"||9||||H||||20031104082400.0000+0100"
    buf += CARRIAGE_RETURN
    buf += END_OF_BLOCK
    buf += CARRIAGE_RETURN


    //val sourceFileName = "./src/main/resources/ADT_ORM_Hl7Messages.txt"
    //val sourceOrig: Source[ByteString, Future[IOResult]] = FileIO.fromPath(Paths.get(sourceFileName), 4000)


    val hl7MllpMessages: Seq[ByteString] = Seq(ByteString(buf.toString())) ++ Seq(ByteString(buf.toString()))
    val sourceOrig =  Source.fromIterator(() => hl7MllpMessages.toIterator).via(connection)
    val flow = sourceOrig.via(connection)
    val closed = flow.runForeach(each => logger.info(s"Client: $id received echo: ${each.utf8String}"))
    closed.onComplete(each => logger.info(s"Client: $id closed: $each"))
  }
}
