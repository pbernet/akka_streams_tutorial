package alpakka.dynamodb



import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.connectors.awsspi.PekkoHttpClient
import org.apache.pekko.stream.connectors.dynamodb.scaladsl.DynamoDb
import org.apache.pekko.stream.scaladsl.{FlowWithContext, Sink, Source}
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.retries.api.RetryStrategy
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

import java.net.URI
import java.util
import java.util.UUID
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*
import scala.util.Try


class DynamoDBEcho(urlWithMappedPort: URI, accessKey: String, secretKey: String, region: String) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  implicit val system: ActorSystem = ActorSystem("DynamoDBEcho")
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  private val testTableName = "testTable"

  val credentialsProvider: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey))
  implicit val client: DynamoDbAsyncClient = createAsyncClient()

  def run(noOfItems: Int): Future[Int] = {
    for {
      _ <- createTable()
      _ <- writeItems(noOfItems)
      result <- readItems(noOfItems)
    } yield result
  }

  // Create a table and read description of this table
  private def createTable() = {
    val source: Source[DescribeTableResponse, NotUsed] = Source
      .single(createTableRequest())
      .via(DynamoDb.flow(parallelism = 1))
      .map(response => DescribeTableRequest.builder().tableName(response.tableDescription.tableName).build())
      .via(DynamoDb.flow(parallelism = 1))

    source
      .wireTap(descTableResponse => logger.info(s"Successfully created table: ${descTableResponse.table.tableName}"))
      .runWith(Sink.ignore)
  }

  private def createTableRequest(): CreateTableRequest = {
    val attributeDefinitions = util.Arrays.asList(
      AttributeDefinition.builder()
        .attributeName("Id")
        .attributeType(ScalarAttributeType.S)
        .build()
    )

    val keySchema = util.Arrays.asList(
      KeySchemaElement.builder()
        .attributeName("Id")
        .keyType(KeyType.HASH)
        .build()
    )

    CreateTableRequest.builder()
      .tableName(testTableName)
      .keySchema(keySchema)
      .attributeDefinitions(attributeDefinitions)
      .provisionedThroughput(ProvisionedThroughput.builder()
        .readCapacityUnits(5L)
        .writeCapacityUnits(5L)
        .build())
      .build()
  }


  private def writeItems(noOfItems: Int) = {
    logger.info(s"About to write $noOfItems items...")

    case class RequestContext(tableName: String, requestId: String)

    val sourceWrite = Source(1 to noOfItems)
      .map(item => {
        val requestId = UUID.randomUUID().toString
        val request = PutItemRequest.builder().tableName(testTableName).item(Map(
          "Id" -> AttributeValue.builder().s(item.toString).build(),
          "att1" -> AttributeValue.builder().s(s"att1-$item").build(),
          "att2" -> AttributeValue.builder().n(s"$item").build()
        ).asJava).build()
        (request, RequestContext(testTableName, requestId))
      })


    val flowWrite: FlowWithContext[PutItemRequest, RequestContext, Try[PutItemResponse], RequestContext, NotUsed] =
      DynamoDb.flowWithContext(parallelism = 1)

    sourceWrite
      .via(flowWrite)
      .map((response: (Try[PutItemResponse], RequestContext)) => {
        val status = response._1.get.sdkHttpResponse().statusCode()
        logger.info(s"Successfully written item. Http response: $status")

      })
      .runWith(Sink.ignore)
  }

  private def readItems(noOfItems: Int) = {
    logger.info(s"About to read 2nd half of all items...")

    val filterExpression = "#att2 > :val"
    val expressionAttrNames = new java.util.HashMap[String, String]()
    expressionAttrNames.put("#att2", "att2")

    val expressionAttrValues = new java.util.HashMap[String, AttributeValue]()
    expressionAttrValues.put(":val", AttributeValue.builder().n((noOfItems / 2).toString).build())

    val scanRequest = ScanRequest.builder()
      .tableName(testTableName) // This hangs when no table is available
      .filterExpression(filterExpression)
      .expressionAttributeNames(expressionAttrNames)
      .expressionAttributeValues(expressionAttrValues)
      .build()

    val scanPageInFlow: Source[ScanResponse, NotUsed] =
      Source
        .single(scanRequest)
        .via(DynamoDb.flowPaginated())

    scanPageInFlow.map { scanResponse =>
      val count = scanResponse.scannedCount()
      val resultCount = scanResponse.items().size()
      logger.info(s"Successfully read $resultCount/$count items")
      scanResponse.items().forEach(item => logger.info(s"Item: $item"))
      resultCount
    }.runWith(Sink.head)
  }

  private def createAsyncClient() = {
    val retryStrategy: RetryStrategy = StandardRetryStrategy.builder().build()

    val client = DynamoDbAsyncClient
      .builder()
      .endpointOverride(urlWithMappedPort)
      .region(Region.of(region))
      .credentialsProvider(credentialsProvider)
      .httpClient(PekkoHttpClient.builder().withActorSystem(system).build())
      // https://pekko.apache.org/docs/pekko-connectors/current/aws-shared-configuration.html
      // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/retries/api/RetryStrategy.html
      .overrideConfiguration(
        ClientOverrideConfiguration
          .builder()
          .retryStrategy(retryStrategy)
          .build())
      .build()
    system.registerOnTermination(client.close())
    client
  }
}
