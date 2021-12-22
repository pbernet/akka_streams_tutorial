package sample.graphstage.loadbalancer

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.stream.stage._

import java.util.concurrent.TimeoutException
import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Stolen from:
  * https://github.com/codeheroesdev/akka-http-lb
  */
class LoadBalancerSlot[T](endpoint: Endpoint, slodId: Int, handleError: (Int, Throwable) => Unit, stopSwitch: Future[Unit],
                          connectionFlow: Flow[HttpRequest, HttpResponse, Any])(implicit mat: ActorMaterializer) extends GraphStage[FlowShape[(HttpRequest, T), (Try[HttpResponse], T)]] {

  private val in = Inlet[(HttpRequest, T)](s"LoadBalancerSlot.$slodId.in")
  private val out = Outlet[(Try[HttpResponse], T)](s"LoadBalancerSlot.$slodId.out")
  private val resetConnectionMessage = "The connection closed with error: Connection reset by peer"

  import mat.executionContext

  override def shape: FlowShape[(HttpRequest, T), (Try[HttpResponse], T)] = FlowShape.of(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {
    private var isConnected: Boolean = false
    private var firstRequest: (HttpRequest, T) = null
    private val inflightRequests = new java.util.ArrayDeque[(HttpRequest, T)]()

    private var connectionFlowSource: SubSourceOutlet[HttpRequest] = _
    private var connectionFlowSink: SubSinkInlet[HttpResponse] = _

    private var strictResponseCallback: AsyncCallback[(Try[HttpResponse], T)] = _

    override def preStart(): Unit = {
      val stopCallback = getAsyncCallback[Option[Throwable]](disconnect)
      stopSwitch.onComplete {
        case Success(_) => stopCallback.invoke(None)
        case Failure(ex) => stopCallback.invoke(Some(ex))
      }

      strictResponseCallback = getAsyncCallback[(Try[HttpResponse], T)] { case (result, t) => push(out, (result, t)) }
    }

    def disconnect(cause: Option[Throwable] = None) = {
      connectionFlowSource.complete()
      if (isConnected) {
        isConnected = false

        val exception = cause.getOrElse(new RuntimeException(s"Connection closed to $endpoint in slot $slodId"))

        emitMultiple(out, inflightRequests.iterator().asScala.map { case (request, t) => (Failure(exception), t) })

        cause.foreach(e => handleError(slodId, e))
        inflightRequests.clear()
      }
    }

    private val connectionOutFlowHandler = new OutHandler {
      override def onPull(): Unit = {
        if (firstRequest != null) {
          inflightRequests.add(firstRequest)
          connectionFlowSource.push(firstRequest._1)
          firstRequest = null
        } else pull(in)
      }

      override def onDownstreamFinish(): Unit = connectionFlowSource.complete()
    }

    private val connectionInFlowHandler = new InHandler {
      override def onPush(): Unit = {
        val response = connectionFlowSink.grab()
        val (_, t) = inflightRequests.pop

        //TODO: This should be fixed with watch completion approach
        response.entity.toStrict(15.seconds).onComplete {
          case Success(entity) => strictResponseCallback.invoke((Success(response withEntity entity), t))
          case Failure(ex) => strictResponseCallback.invoke(Failure(ex), t)
        }
      }

      override def onUpstreamFinish(): Unit = disconnect()

      override def onUpstreamFailure(ex: Throwable): Unit = ex match {
        case t: TimeoutException => disconnect()
        case t: StreamTcpException if t.getMessage == resetConnectionMessage => disconnect()
        case _ => disconnect(Some(ex))
      }
    }

    override def onPush(): Unit = {
      def establishConnectionFlow() = {
        connectionFlowSource = new SubSourceOutlet[HttpRequest]("LoadbBalancerSlot.subSource")
        connectionFlowSource.setHandler(connectionOutFlowHandler)

        connectionFlowSink = new SubSinkInlet[HttpResponse]("LoadBalancerSlot.subSink")
        connectionFlowSink.setHandler(connectionInFlowHandler)

        isConnected = true

        Source.fromGraph(connectionFlowSource.source).via(connectionFlow).runWith(Sink.fromGraph(connectionFlowSink.sink))(subFusingMaterializer)
        connectionFlowSink.pull()
      }

      val (request, t) = grab(in)

      if (isConnected) {
        inflightRequests.add((request, t))
        connectionFlowSource.push(request)
      } else {
        firstRequest = (request, t)
        establishConnectionFlow()
      }
    }

    override def onPull(): Unit = {
      if (isConnected) connectionFlowSink.pull()
      else if (!hasBeenPulled(in)) pull(in)
    }

    setHandler(in, this)
    setHandler(out, this)
  }

}