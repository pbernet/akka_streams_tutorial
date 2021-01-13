package alpakka.slick

import akka.actor.ActorSystem
import akka.stream.alpakka.slick.scaladsl.{SlickSession, _}
import akka.stream.scaladsl._
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.slf4j.{Logger, LoggerFactory}
import slick.jdbc.GetResult

import scala.concurrent.Future

/**
  * DB access via Slick
  *
  * @param urlWithMappedPortSlick
  */
class SlickRunner(urlWithMappedPortSlick: String) {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("SlickRunner")
  implicit val executionContext = system.dispatcher

  //Tweak config url param dynamically with mapped port from testontainer
  val tweakedConf =  ConfigFactory.empty()
    .withValue("slick-postgres.db.url", ConfigValueFactory.fromAnyRef(urlWithMappedPortSlick))
    .withFallback(ConfigFactory.load())

  implicit val session = SlickSession.forConfig("slick-postgres", tweakedConf)

  import session.profile.api._

  case class User(id: Int, name: String)
  class Users(tag: Tag) extends Table[(Int, String)](tag, "USERS") {
    def id = column[Int]("ID")
    def name = column[String]("NAME")
    def * = (id, name)
  }

  implicit val getUserResult = GetResult(r => User(r.nextInt, r.nextString))

  val createTable = sqlu"""CREATE TABLE USERS(ID INTEGER, NAME VARCHAR(50))"""
  val dropTable = sqlu"""DROP TABLE USERS"""
  val selectAllUsers = sql"SELECT ID, NAME FROM USERS".as[User]
  val typedSelectAllUsers = TableQuery[Users].result

  def insertUser(user: User): DBIO[Int] =
    sqlu"INSERT INTO USERS VALUES(${user.id}, ${user.name})"

  def getAllUsersFromDb: Future[Set[User]] = Slick.source(selectAllUsers).runWith(Sink.seq).map(_.toSet)

  def createTableOnSession() = {
//    Await.result(session.db.run(createTable), 10.seconds)
    session.db.run(createTable)
  }

  def dropTableOnSession() = {
//    Await.result(session.db.run(dropTable), 10.seconds)
    session.db.run(dropTable)
  }

  def populate(noOfUsers: Int = 100) = {
    logger.info("About to populate...")
    val users = (1 to noOfUsers).map(i => User(i, s"Name$i")).toSet
    val actions = users.map(insertUser)

    // This uses the standard Slick API exposed by the Slick session
    // on purpose, just to double-check that inserting data through
    // our Alpakka connectors is equivalent to inserting it the Slick way.
    session.db.run(DBIO.seq(actions.toList: _*))
    logger.info("Populated with: {} users", users.size)
  }

  def close() = {
    system.registerOnTermination(() => session.close())
  }

  def read() = {
    logger.info("About to read...")
    val done = getAllUsersFromDb
    done.onComplete(result => logger.info(s"Done: ${result}"))
  }
}

object SlickRunner extends App {
  def apply(urlWithMappedPortSlick: String) = new SlickRunner(urlWithMappedPortSlick)
}