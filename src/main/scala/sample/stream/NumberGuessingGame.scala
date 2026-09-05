package sample.stream

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.*
import org.apache.pekko.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.io.StdIn
import scala.util.Random

/**
  * Generated with: https://claude.ai/chat
  * Prompt: Generate a game with pekko streams
  *
  * This is the first attempt 1:1 copy/paste
  * A decent explanation was delivered as well
  * Pretty cool
  */
object NumberGuessingGame {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("NumberGuessingGame")
    implicit val ec: ExecutionContext = system.dispatcher

    val targetNumber = Random.nextInt(100) + 1 // Random number between 1 and 100
    var attempts = 0

    def gameLogic(guess: Int): String = {
      attempts += 1
      if (guess < targetNumber) {
        "Too low! Try again."
      } else if (guess > targetNumber) {
        "Too high! Try again."
      } else {
        s"Congratulations! You guessed the number $targetNumber in $attempts attempts."
      }
    }

    val guessSource: Source[Int, NotUsed] = Source.unfold(()) { _ =>
      print("Enter your guess (1-100), or 'q' to quit: ")
      val input = StdIn.readLine()
      if (input.toLowerCase == "q") {
        None
      } else {
        Some((), input.toInt)
      }
    }

    val gameFlow: Flow[Int, String, NotUsed] = Flow[Int].map(gameLogic)

    val printSink: Sink[String, Future[Done]] = Sink.foreach[String](println)

    val gameGraph: RunnableGraph[Future[Done]] = guessSource
      .via(gameFlow)
      .takeWhile(result => !result.startsWith("Congratulations"), inclusive = true)
      .toMat(printSink)(Keep.right)

    val gameFuture: Future[Done] = gameGraph.run()

    gameFuture.onComplete { _ =>
      println("Game over!")
      system.terminate()
    }
  }
}
