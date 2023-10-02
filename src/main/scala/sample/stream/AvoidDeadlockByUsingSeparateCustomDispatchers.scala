package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.ClosedShape
import org.apache.pekko.stream.scaladsl.GraphDSL.Implicits._
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString

import java.io.{BufferedInputStream, BufferedOutputStream}
import java.nio.file.Paths
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * Reproducer and implemented solution of this discussion:
  * https://stackoverflow.com/questions/71264640/akka-stream-hangs-when-starting-more-than-15-external-processes-using-processbui
  *
  * See also other examples with custom-dispatchers
  */
object AvoidDeadlockByUsingSeparateCustomDispatchers extends App {
  implicit val system: ActorSystem = ActorSystem()

  import system.dispatcher

  // This may be bigger than the pool size of the custom-dispatchers
  val PROCESSES_COUNT = 50

  println(s"Running with $PROCESSES_COUNT processes...")

  def executeCmdOnStream(cmd: String): Flow[ByteString, ByteString, _] = {
    val convertProcess = new ProcessBuilder(cmd).start
    val pipeIn = new BufferedOutputStream(convertProcess.getOutputStream)
    val pipeOut = new BufferedInputStream(convertProcess.getInputStream)
    Flow
      .fromSinkAndSource(
        // The important thing is to use two *different* (custom) dispatchers to avoid the dead lock
        // The chosen ones do the job, but may not be optimal
        StreamConverters.fromOutputStream(() => pipeIn).async("custom-dispatcher-fork-join"),
        StreamConverters.fromInputStream(() => pipeOut).async("custom-dispatcher-for-blocking"))
  }

  val source = Source(1 to 100)
    .map(element => {
      println(s"--emit: $element")
      ByteString(element)
    })

  val sinksList = (1 to PROCESSES_COUNT).map(i => {
    Flow[ByteString]
      .via(executeCmdOnStream("cat"))
      .toMat(FileIO.toPath(Paths.get(s"process-$i.txt")))(Keep.right)
  })

  val graph = GraphDSL.create(sinksList) { implicit builder =>
    sinks =>

      val broadcast = builder.add(Broadcast[ByteString](sinks.size))
      source ~> broadcast.in
      for (i <- broadcast.outlets.indices) {
        broadcast.out(i) ~> sinks(i)
      }
      ClosedShape
  }

  Await.result(Future.sequence(RunnableGraph.fromGraph(graph).run()), Duration.Inf)
}