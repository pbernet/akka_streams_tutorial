package alpakka.slick

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.{SlickSession, _}
import akka.stream.scaladsl._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.{GetResult, ResultSetConcurrency, ResultSetType}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future}

/**
  * DB access via Slick
  * Run with integration test: alpakka.slick.SlickIT
  *
  */
class SlickRunner(urlWithMappedPort: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SlickRunner")
  implicit val executionContext = system.dispatcher

  val counter = new AtomicInteger()

  system.registerOnTermination(() => {
    logger.info("About to close session...")
    session.close()
  })

  // Tweak config url param dynamically with mapped port from container
  val tweakedConf =  ConfigFactory.empty()
    .withValue("slick-postgres.db.url", ConfigValueFactory.fromAnyRef(urlWithMappedPort))
    .withFallback(ConfigFactory.load())

  implicit val session = SlickSession.forConfig("slick-postgres", tweakedConf)

  import session.profile.api._

  case class User(id: Int, name: String)
  class Users(tag: Tag) extends Table[(Int, String)](tag, Some("public"),"users") {
    def id = column[Int]("id")
    def name = column[String]("name")
    def * = (id, name)
  }

  implicit val getUserResult = GetResult(r => User(r.nextInt(), r.nextString()))

  val createTable = sqlu"""CREATE TABLE public.users(id INTEGER, name VARCHAR(50))"""
  val dropTable = sqlu"""DROP TABLE public.users"""
  val selectAllUsers = sql"SELECT id, name FROM public.users".as[User]

  val typedSelectAllUsers = TableQuery[Users]
    .result
    .withStatementParameters(
      rsType = ResultSetType.ForwardOnly,
      rsConcurrency = ResultSetConcurrency.ReadOnly,
      fetchSize = 10
    )
    .transactionally

  def insertUser(user: User): DBIO[Int] =
    sqlu"INSERT INTO public.users VALUES(${user.id}, ${user.name})"

  def getAllUsersFromDb: Future[Set[User]] = Slick.source(selectAllUsers).runWith(Sink.seq).map(_.toSet)

  def readUsers() = {
    logger.info("About to read...")
    val result = Await.result(getAllUsersFromDb, 10.seconds)
    logger.info(s"Successfully read: ${result.size} users")
    result
  }

  def processUsersPaged() = {
     val done = Slick.source(typedSelectAllUsers)
       .grouped(1000)
       .wireTap((group: Seq[(Int, String)]) => {
         // Simulate some processing on each group
         val sum = group.map(_._1).sum
         logger.info(s"Group PK sum: $sum")
       })

       .map(each => counter.getAndAdd(each.size))
       .runWith(Sink.ignore)

    done.onComplete(done => logger.info(s"Done reading paged: $done with: ${counter.get()} elements"))
    done
  }

  def createTableOnSession() = {
    Await.result(session.db.run(createTable), 2.seconds)
  }

  def dropTableOnSession() = {
    Await.result(session.db.run(dropTable), 2.seconds)
  }

  def populate(noOfUsers: Int = 100) = {
    logger.info(s"About to populate DB with: $noOfUsers users...")
    val users = (1 to noOfUsers).map(i => User(i, s"Name$i")).toSet
    val actions = users.map(insertUser)

    // Insert via standard Slick API, this may take some time
    Await.result(session.db.run(DBIO.seq(actions.toList: _*)), 60.seconds)
    logger.info("Done populating DB")
  }

  def terminate() = {
    system.terminate()
  }
}

object SlickRunner extends App {
  def apply(urlWithMappedPort: String) = new SlickRunner(urlWithMappedPort)
}