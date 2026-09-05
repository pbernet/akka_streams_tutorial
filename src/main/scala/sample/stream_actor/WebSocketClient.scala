package sample.stream_actor

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.{ActorRef, ActorSystem}
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.model.ws.*
import org.apache.pekko.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import org.apache.pekko.stream.{FlowShape, Graph, SourceShape}
import sample.stream_actor.WindTurbineSimulator.*

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketClient {
  def apply(id: String, endpoint: String, windTurbineSimulator: ActorRef)
           (using
            system: ActorSystem,
            executionContext: ExecutionContext): WebSocketClient = {
    new WebSocketClient(id, endpoint, windTurbineSimulator)
  }
}

class WebSocketClient(id: String, endpoint: String, windTurbineSimulator: ActorRef)
                     (using
                      system: ActorSystem,
                      executionContext: ExecutionContext) {


  val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = {
    val websocketUri = s"$endpoint/measurements/$id"
    Http().webSocketClientFlow(WebSocketRequest(websocketUri))
  }

  val outgoing: Graph[SourceShape[TextMessage.Strict], NotUsed] = GraphDSL.create() { implicit builder =>
    val data = WindTurbineData(id)

    val flow = builder.add {
      Source.tick(1.second, 100.millis,())  //valve for the WindTurbineData frequency
        .map(_ => TextMessage(data.getNext))
    }

    SourceShape(flow.out)
  }

  val incoming: Graph[FlowShape[Message, Unit], NotUsed] = GraphDSL.create() { implicit builder =>
    val flow = builder.add {
      Flow[Message]
        .collect {
          case TextMessage.Strict(text) =>
            Future.successful(text)
          case TextMessage.Streamed(textStream) =>
            textStream.runFold("")(_ + _)
              .flatMap(Future.successful)
        }
        .mapAsync(1)(identity)
        .map(each => println(s"Client received msg: $each"))
    }

    FlowShape(flow.in, flow.out)
  }

  val (upgradeResponse, closed) = Source.fromGraph(outgoing)
    .viaMat(webSocketFlow)(Keep.right) // keep the materialized Future[WebSocketUpgradeResponse]
    .via(incoming)
    .toMat(Sink.ignore)(Keep.both) // also keep the Future[Done]
    .run()


  val connected: Future[Unit] =
    upgradeResponse.map { upgrade =>
      upgrade.response.status match {
        case StatusCodes.SwitchingProtocols => windTurbineSimulator ! Upgraded
        case statusCode => windTurbineSimulator ! FailedUpgrade(statusCode)
      }
    }

  connected.onComplete {
    case Success(_) => windTurbineSimulator ! Connected
    case Failure(ex) => windTurbineSimulator ! ConnectionFailure(ex)
  }

  closed.map { _ =>
    windTurbineSimulator ! Terminated
  }
  closed.onComplete {
    case Success(_)  => windTurbineSimulator ! Connected
    case Failure(ex) => windTurbineSimulator ! ConnectionFailure(ex)
  }
}
