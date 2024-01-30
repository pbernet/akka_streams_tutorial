package akkahttp.oidc

import dasniko.testcontainers.keycloak.KeycloakContainer
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.{HttpChallenge, OAuth2BearerToken}
import org.apache.pekko.http.scaladsl.model.{ContentTypes, HttpEntity, HttpRequest, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.server.{AuthenticationFailedRejection, Directive1, RejectionHandler, Route}
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.util.Timeout
import org.keycloak.TokenVerifier
import org.keycloak.adapters.KeycloakDeploymentBuilder
import org.keycloak.admin.client.{CreatedResponseUtil, Keycloak, KeycloakBuilder}
import org.keycloak.jose.jws.AlgorithmType
import org.keycloak.representations.AccessToken
import org.keycloak.representations.adapters.config.AdapterConfig
import org.keycloak.representations.idm.{ClientRepresentation, CredentialRepresentation, UserRepresentation}
import org.slf4j.{Logger, LoggerFactory}

import java.math.BigInteger
import java.nio.file.{Files, Paths}
import java.security.spec.RSAPublicKeySpec
import java.security.{KeyFactory, PublicKey}
import java.time.Duration
import java.util
import java.util.{Base64, Collections}
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.sys.process.{Process, stringSeqToProcess}
import scala.util.{Failure, Success}

/**
  * A "one-click" Keycloak OIDC server with akka-http frontend
  *
  * Inspired by:
  * https://scalac.io/blog/user-authentication-keycloak-1
  *
  * Uses a HTML5 client:
  * https://github.com/keycloak/keycloak/tree/main/examples/js-console
  * instead of the separate React client
  *
  * Runs with:
  * https://github.com/dasniko/testcontainers-keycloak
  * automatically configured for convenience
  *
  * Doc:
  * https://www.keycloak.org/docs/latest/securing_apps/#_javascript_adapter
  * https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/security-directives/index.html
  *
  */
object OIDCKeycloak extends App with CORSHandler with JsonSupport {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def runKeycloak() = {
    // Pin to same version as "keycloakVersion" in build.sbt
    val keycloak = new KeycloakContainer("quay.io/keycloak/keycloak:23.0.5")
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
      val keycloakAdminClient = KeycloakBuilder.builder()
        .serverUrl(keycloak.getAuthServerUrl())
        .realm("master")
        .clientId(adminClientId)
        .username(keycloak.getAdminUsername())
        .password(keycloak.getAdminPassword())
        .build()
      logger.info("Connected to Keycloak server version: " + keycloakAdminClient.serverInfo().getInfo().getSystemInfo().getVersion())
      keycloakAdminClient
    }

    def createTestUser(keycloakAdminClient: Keycloak) = {
      val username = "test"
      val password = "test"
      val usersResource = keycloakAdminClient.realm("test").users()

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
      logger.info(s"User $username/$password may sign in via: http://localhost:${keycloak.getHttpPort}/realms/test/account")
    }

    def createClientConfig(keycloakAdminClient: Keycloak) = {
      val clientId = "my-test-client"
      val clientRepresentation = new ClientRepresentation()
      clientRepresentation.setClientId(clientId)
      clientRepresentation.setProtocol("openid-connect")

      val redirectUriTestingOnly = new util.ArrayList[String]()
      redirectUriTestingOnly.add("http://127.0.0.1:6002/*")
      clientRepresentation.setRedirectUris(redirectUriTestingOnly)
      val webOriginsTestingOnly = new util.ArrayList[String]()
      webOriginsTestingOnly.add("*")
      clientRepresentation.setWebOrigins(webOriginsTestingOnly)

      val resp = keycloakAdminClient.realm("test").clients().create(clientRepresentation)
      logger.info(s"Successfully created client config for clientId: $clientId, response status: " + resp.getStatus)

      val clients: util.List[ClientRepresentation] = keycloakAdminClient.realm("test").clients().findByClientId(clientId)
      logger.info(s"Successfully read ClientRepresentation for clientId: ${clients.get(0).getClientId}")
    }

    val keycloakAdminClient = initAdminClient()
    createTestUser(keycloakAdminClient)
    createClientConfig(keycloakAdminClient)
    keycloakAdminClient
  }

  def runBackendServer(keycloak: KeycloakContainer) = {

    implicit def rejectionHandler = RejectionHandler.newBuilder().handle {
      case AuthenticationFailedRejection(reason, _) => complete(StatusCodes.Unauthorized, reason.toString)
    }.result().mapRejectionResponse(addCORSHeaders)

    implicit val timeout: Timeout = Timeout(5.seconds)

    val config = new AdapterConfig()
    config.setAuthServerUrl(keycloak.getAuthServerUrl)
    config.setRealm("test")
    config.setResource("my-test-client")
    val keycloakDeployment = KeycloakDeploymentBuilder.build(config)
    logger.info("Dynamic authServerBaseUrl: " + keycloakDeployment.getAuthServerBaseUrl)


    def generateKey(keyData: KeyData): PublicKey = {
      val keyFactory = KeyFactory.getInstance(AlgorithmType.RSA.toString)
      val urlDecoder = Base64.getUrlDecoder
      val modulus = new BigInteger(1, urlDecoder.decode(keyData.n))
      val publicExponent = new BigInteger(1, urlDecoder.decode(keyData.e))
      keyFactory.generatePublic(new RSAPublicKeySpec(modulus, publicExponent))
    }

    val publicKeys: Future[Map[String, PublicKey]] =
      Http().singleRequest(HttpRequest(uri = keycloakDeployment.getJwksUrl)).flatMap(response => {
        Unmarshal(response).to[Keys].map(_.keys.map(k => (k.kid, generateKey(k))).toMap)
      })


    // Alternative:
    // https://doc.akka.io/docs/akka-http/current/routing-dsl/directives/security-directives/authenticateOAuth2.html
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
              val usersOrig = adminClient.realm("test").users().list().asScala
              val usersBasic = UsersKeycloak(usersOrig.collect(each => UserKeycloak(Option(each.getFirstName), Option(each.getLastName), Option(each.getEmail))).toSeq)
              complete(usersBasic)
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

    val routes: Route = corsHandler(userRoutes) ~ getFromDocRoot
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
    else if (os == "windows 10") Seq("cmd", "/c", s"start http://127.0.0.1:6002").!
  }


  val keycloak = runKeycloak()
  val adminClient = configureKeycloak(keycloak)
  adminConsole(keycloak.getAuthServerUrl)
  runBackendServer(keycloak)
  browserClient()
  Thread.sleep(100000)
}
