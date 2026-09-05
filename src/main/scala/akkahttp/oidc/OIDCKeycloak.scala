package akkahttp.oidc

import dasniko.testcontainers.keycloak.KeycloakContainer
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{HttpChallenge, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.{AuthenticationFailedRejection, Directive1, Route}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.keycloak.TokenVerifier
import org.keycloak.admin.client.{CreatedResponseUtil, Keycloak}
import org.keycloak.jose.jws.AlgorithmType
import org.keycloak.representations.AccessToken
import org.keycloak.representations.idm.{ClientRepresentation, CredentialRepresentation, UserRepresentation}
import org.slf4j.{Logger, LoggerFactory}

import java.math.BigInteger
import java.nio.file.{Files, Paths}
import java.security.spec.RSAPublicKeySpec
import java.security.{KeyFactory, PublicKey}
import java.time.Duration
import java.util
import java.util.{Base64, Collections}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}


/**
  * A "one-click" Keycloak OIDC server with pekko-http frontend.
  * The pekko-http endpoint /users loads all users from the Keycloak server.
  *
  * Inspired by:
  * https://scalac.io/blog/user-authentication-keycloak-1
  *
  * Uses a HTML5 client: src/main/resources/KeycloakClient.html
  * instead of the separate React client
  *
  * Runs with:
  * https://github.com/dasniko/testcontainers-keycloak
  * automatically configured for convenience
  *
  * Doc:
  * https://www.keycloak.org/docs/latest/securing_apps/#_javascript_adapter
  * https://pekko.apache.org/docs/pekko-http/1.0/routing-dsl/directives/security-directives/index.html
  */
object OIDCKeycloak extends CORSHandler with JsonSupport {
  def main(args: Array[String]): Unit = {
    val runningKeycloak = keycloak
    val _ = adminClient
    adminConsole(runningKeycloak.getAuthServerUrl)
    runBackendServer(runningKeycloak)
    browserClient()
    Thread.sleep(100000)
  }
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val CLIENT_ID = "my-test-client"
  private val REALM_NAME = "test"

  implicit lazy val system: ActorSystem = ActorSystem()
  implicit lazy val executionContext: ExecutionContextExecutor = system.dispatcher

  def runKeycloak() = {
    // Pin to same version as "keycloakVersion" in build.sbt
    val keycloak = new KeycloakContainer("quay.io/keycloak/keycloak:26.3")
      // Keycloak config taken from:
      // https://github.com/keycloak/keycloak/blob/main/examples/js-console/example-realm.json
      .withRealmImportFile("keycloak_realm_config.json")
      .withStartupTimeout(Duration.ofSeconds(180))

    keycloak.start()
    logger.info("Running Keycloak on URL: {}", keycloak.getAuthServerUrl)
    keycloak
  }

  def configureKeycloak(keycloak: KeycloakContainer) = {
    val adminClientId = "admin-cli"

    def initAdminClient() = {
      val keycloakAdminClient = keycloak.getKeycloakAdminClient()
      logger.info("Connected to Keycloak server version: {}", keycloakAdminClient.serverInfo().getInfo.getSystemInfo.getVersion)
      keycloakAdminClient
    }

    def createTestUser(keycloakAdminClient: Keycloak): Unit = {
      val username = "test"
      val password = "test"
      val usersResource = keycloakAdminClient.realm(REALM_NAME).users()

      val user = new UserRepresentation()
      user.setEnabled(true)
      user.setUsername(username)
      user.setFirstName("First")
      user.setLastName("Last")
      user.setEmail(s"$username@test.local")
      user.setAttributes(Collections.singletonMap("origin", util.Arrays.asList(adminClientId)))

      // Create user
      val response = usersResource.create(user)
      val userId = CreatedResponseUtil.getCreatedId(response)

      // Define password credential
      val passwordCred = new CredentialRepresentation()
      passwordCred.setTemporary(false)
      passwordCred.setType(CredentialRepresentation.PASSWORD)
      passwordCred.setValue(password)

      // Set password credential
      val userResource = usersResource.get(userId)
      userResource.resetPassword(passwordCred)

      logger.info(s"User $username created with userId: $userId")
      logger.info(s"User $username/$password may sign in via: http://localhost:${keycloak.getHttpPort}/realms/$REALM_NAME/account")
    }

    def createClientConfig(keycloakAdminClient: Keycloak): Unit = {
      val clientRepresentation = new ClientRepresentation()
      clientRepresentation.setClientId(CLIENT_ID)
      clientRepresentation.setProtocol("openid-connect")

      val redirectUriTestingOnly = new util.ArrayList[String]()
      redirectUriTestingOnly.add("http://127.0.0.1:6002/*")
      clientRepresentation.setRedirectUris(redirectUriTestingOnly)
      val webOriginsTestingOnly = new util.ArrayList[String]()
      webOriginsTestingOnly.add("*")
      clientRepresentation.setWebOrigins(webOriginsTestingOnly)

      val resp = keycloakAdminClient.realm(REALM_NAME).clients().create(clientRepresentation)
      logger.info(s"Successfully created client config for clientId: $CLIENT_ID, response status: " + resp.getStatus)

      val clients: util.List[ClientRepresentation] = keycloakAdminClient.realm(REALM_NAME).clients().findByClientId(CLIENT_ID)
      logger.info(s"Successfully read ClientRepresentation for clientId: ${clients.get(0).getClientId}")
    }

    val keycloakAdminClient = initAdminClient()
    createTestUser(keycloakAdminClient)
    createClientConfig(keycloakAdminClient)
    keycloakAdminClient
  }

  def runBackendServer(keycloak: KeycloakContainer): Unit = {
    val authServerUrl = keycloak.getAuthServerUrl
    val realmUrl = s"$authServerUrl/realms/$REALM_NAME"
    val jwksUrl = s"$realmUrl/protocol/openid-connect/certs"

    logger.info(s"Realm URL: $realmUrl")
    logger.info(s"JSON Web Key Set (JWKS) URL: $jwksUrl")
    logger.info(s"Client ID: $CLIENT_ID")


    def generateKey(keyData: KeyData): PublicKey = {
      val keyFactory = KeyFactory.getInstance(AlgorithmType.RSA.toString)
      val urlDecoder = Base64.getUrlDecoder
      val modulus = new BigInteger(1, urlDecoder.decode(keyData.n))
      val publicExponent = new BigInteger(1, urlDecoder.decode(keyData.e))
      keyFactory.generatePublic(new RSAPublicKeySpec(modulus, publicExponent))
    }

    val publicKeys: Future[Map[String, PublicKey]] =
      Http().singleRequest(HttpRequest(uri = jwksUrl)).flatMap(response => {
        val json = Unmarshal(response).to[String]
        val keys = json.map { jsonString =>
          decode[Keys](jsonString) match {
            case Right(keys) => keys
            case Left(error) => throw new RuntimeException(error.getMessage)
          }
        }
        keys.map(_.keys.map(k => (k.kid, generateKey(k))).toMap)
      })

    // Alternative impl to:
    // https://pekko.apache.org/docs/pekko-http/current/routing-dsl/directives/security-directives/authenticateOAuth2.html
    def authenticate: Directive1[AccessToken] =
      extractCredentials.flatMap {
        case Some(OAuth2BearerToken(token)) =>
          onComplete(verifyToken(token)).flatMap {
            case Success(Some(t)) =>
              logger.info(s"Token: '${token.take(10)}...' is valid")
              provide(t)
            case _ =>
              logger.warn(s"Token: '${token.take(10)}...' is not valid")
              reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsRejected, HttpChallenge("JWT", None)))
          }
        case _ =>
          logger.warn("No token present in request")
          reject(AuthenticationFailedRejection(AuthenticationFailedRejection.CredentialsMissing, HttpChallenge("JWT", None)))
      }

    def verifyToken(token: String): Future[Option[AccessToken]] = {
      logger.info(s"About to verify token...")
      val tokenVerifier = TokenVerifier.create(token, classOf[AccessToken])
      for {
        publicKey <- publicKeys.map(_.get(tokenVerifier.getHeader.getKeyId))
      } yield publicKey match {
        case Some(pk) =>
          val token = tokenVerifier.publicKey(pk).verify().getToken
          Some(token)
        case None =>
          logger.warn(s"No public key found for id: ${tokenVerifier.getHeader.getKeyId}")
          None
      }
    }


    val userRoutes: Route =
      logRequest("log request") {
        path("users") {
          get {
            authenticate { token =>
              // To have "real data": Read 'UserRepresentation' from Keycloak via the admin client and then strip down
              val usersOrig = adminClient.realm(REALM_NAME).users().list().asScala
              val usersBasic = UsersKeycloak(usersOrig.collect(each => UserKeycloak(Option(each.getFirstName), Option(each.getLastName), Option(each.getEmail))).toSeq)
              complete(HttpResponse(StatusCodes.OK, entity = usersBasic.asJson.noSpaces))
            }
          }
        }
      }

    val getFromDocRoot: Route =
      get {
        concat(
          pathSingleSlash {
            val content = new String(Files.readAllBytes(Paths.get("src/main/resources/KeycloakClient.html")))
            val renderedPage = content.replaceAll("%%PORT%%", keycloak.getFirstMappedPort.toString)
            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, renderedPage))
          }
        )
      }

    val getResource: Route =
      pathPrefix("js") {
        path(Remaining) { file =>
          getFromFile(s"src/main/resources/js/$file")
        }
      }

    val routes: Route = corsHandler(userRoutes) ~ getFromDocRoot ~ getResource
    val bindingFuture = Http().newServerAt("127.0.0.1", 6002).bind(routes)

    bindingFuture.onComplete {
      case Success(b) =>
        logger.info(s"Http server started, listening on: http:/${b.localAddress}")
      case Failure(e) =>
        logger.info(s"Server could not bind to... Exception message: ${e.getMessage}")
        system.terminate()
    }
  }


  // Login with admin/admin
  def adminConsole(keycloakURL: String) = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open $keycloakURL").!
  }

  // Login with test/test
  def browserClient() = {
    val os = System.getProperty("os.name").toLowerCase
    if (os == "mac os x") Process(s"open http://127.0.0.1:6002").!
    else if (os.startsWith("windows")) Seq("cmd", "/c", s"start http://127.0.0.1:6002").!
  }


  lazy val keycloak = runKeycloak()
  lazy val adminClient = configureKeycloak(keycloak)
}
