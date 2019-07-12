package sample.stream_actor

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, FlowShape, SourceShape}
import sample.stream_actor.WindTurbineSimulator._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object WebSocketClient {
  def apply(id: String, endpoint: String, windTurbineSimulator: ActorRef)
           (implicit
            system: ActorSystem,
            materializer: ActorMaterializer,
            executionContext: ExecutionContext) = {
    new WebSocketClient(id, endpoint, windTurbineSimulator)(system, materializer, executionContext)
  }
}

class WebSocketClient(id: String, endpoint: String, windTurbineSimulator: ActorRef)
                     (implicit
                      system: ActorSystem,
                      materializer: ActorMaterializer,
                      executionContext: ExecutionContext) {


  val webSocketFlow: Flow[Message, Message, Future[WebSocketUpgradeResponse]] = {
    val websocketUri = s"$endpoint/measurements/$id"
    Http().webSocketClientFlow(WebSocketRequest(websocketUri))
  }

  val outgoing = GraphDSL.create() { implicit builder =>
    val data = WindTurbineData(id)

    val flow = builder.add {
      Source.tick(1.second, 100.millis,())  //valve for the WindTurbineData frequency
        .map(_ => TextMessage(data.getNext))
    }

    SourceShape(flow.out)
  }

  val incoming = GraphDSL.create() { implicit builder =>
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


  val connected =
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
