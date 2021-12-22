package sample.loadbalancer.playground

import akka.actor.ActorSystem
import com.dimafeng.testcontainers.GenericContainer
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.containers.wait.strategy.Wait

/**
  * Bootstrap containers manually
  * Uses dimafeng containers - needed?
  *
  *
  */
object LoadBalancerUmbrella extends App {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  implicit val system = ActorSystem("LoadBalancerUmbrella")
  implicit val executionContext = system.dispatcher


  val ocrContainer1 = GenericContainer("nassiesse/simple-java-ocr:latest",
    exposedPorts = Seq(8081),
    waitStrategy = Wait.forLogMessage(".*Tomcat started on port.*", 1))
  ocrContainer1.start()
  logger.info(s"OCR 1 container listening on: ${ocrContainer1.underlyingUnsafeContainer.getMappedPort(8081)}")


  val ocrContainer2 = GenericContainer("nassiesse/simple-java-ocr:latest",
    exposedPorts = Seq(8082),
    waitStrategy = Wait.forLogMessage(".*Tomcat started on port.*", 1))
  ocrContainer2.start()
  logger.info(s"OCR 2 container listening on: ${ocrContainer2.underlyingUnsafeContainer.getMappedPort(8082)}")


  //Doc testcontainers-scala
  //https://github.com/testcontainers/testcontainers-scala
  // Can not be configured at runtime, because of dynamic ports
  // only at build time via Dockerfile see example...
  val nginxContainer = GenericContainer("nginx:latest",
    exposedPorts = Seq(80),
    waitStrategy = Wait.forHttp("/"))
  nginxContainer.start()
  logger.info(s"NGINX container listening on: ${nginxContainer.underlyingUnsafeContainer.getMappedPort(80)}")
  nginxContainer.configure { c =>
    //c.copyFileToContainer()
    c.execInContainer("ls -lisa")
  }



  //  val nginxScala = new NginxContainer()
  //  nginxScala.start()
  //  logger.info(s"nginxScala container listening on: ${nginxScala.underlyingUnsafeContainer.getMappedPort(1111)}")
  //  nginxScala.configure { c =>
  //  //c.copyFileToContainer()
  //  c.execInContainer("ls -lisa")}


}
