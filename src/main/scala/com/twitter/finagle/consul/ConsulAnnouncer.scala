package com.twitter.finagle.consul

import com.twitter.finagle.Http
import com.twitter.util.{Future, FuturePool, Time, TimerTask}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.{Announcer, Announcement}
import com.twitter.conversions.time._
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import java.nio.charset.StandardCharsets._
import java.util.UUID.randomUUID
import java.util.logging.Logger
import scala.collection.mutable.{Map => MutableMap}

class ConsulAnnouncerException(msg: String) extends Exception(msg)

class ConsulAnnouncer extends Announcer {

  val scheme = "consul"
  val registerPath = "/v1/catalog/register"
  val deregisterPath = "/v1/catalog/deregister"
  val logger = Logger.getLogger(getClass.getName)

  case class ConsulService(id: String, name: String, address: String, port: Int)
  case class ConsulNode(
    dc: Option[String],
    name: String,
    address: String,
    service: ConsulService
  )

  private val timer = DefaultTimer.twitter
  private val futurePool = FuturePool.unboundedPool
  private val timerTasks = MutableMap.empty[String, TimerTask]

  // xxx: implement optional datacenter param
  // xxx: implement health check and tags
  // xxx: implement multi-hosts support
  def register(hosts: String, node: ConsulNode): Future[Boolean] = {
    val client = Http.newClient(hosts)
    val payload = s"""{
      "Node": "${node.name}",
      "Address": "${node.address}",
      "Service": {
        "ID": "${node.service.id}",
        "Tags": ["finagle"],
        "Service": "${node.service.name}",
        "Address": "${node.service.address}",
        "Port": ${node.service.port}
      },
      "Check": {
        "Node": "${node.name}",
        "CheckID": "service:${node.service.id}",
        "Status": "passing",
        "ServiceID": "${node.service.id}",
        "Ttl": "30s"
      }
    }"""
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, registerPath)
    val ttl = 55.seconds // xxx: should be configurable
    req.setContent(ChannelBuffers.copiedBuffer(payload, UTF_8))
    logger.info(s"Register consul service ${node.service}")
    // xxx: timeout?
    // xxx: request error?
    client.toService(req) map { resp =>
      val wasSuccessful = resp.getStatus.getCode == 200
      if(wasSuccessful) {
        val ttask = timer.schedule(ttl.fromNow, ttl) {
          // reregister service to deal with health-check TTL
          futurePool(prolongate(hosts, node))
        }
        synchronized {
          timerTasks += (node.service.id -> ttask)
        }
      }
      wasSuccessful
    }
  }

  // xxx: code duplication!
  def prolongate(hosts: String, node: ConsulNode): Future[Unit] = {
    val payload = s"""{
      "Node": "${node.name}",
      "Address": "${node.address}",
      "Service": {
        "ID": "${node.service.id}",
        "Tags": ["finagle"],
        "Service": "${node.service.name}",
        "Address": "${node.service.address}",
        "Port": ${node.service.port}
      },
      "Check": {
        "Node": "${node.name}",
        "CheckID": "service:${node.service.id}",
        "Status": "passing",
        "ServiceID": "${node.service.id}",
        "Ttl": "60s"
      }
    }"""
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, registerPath)
    req.setContent(ChannelBuffers.copiedBuffer(payload, UTF_8))
    // xxx: debug log only
    logger.info(s"Prolongate consul service ${node.service}")
    val client = Http.newClient(hosts)
    // xxx: timeout?
    // xxx: response error?
    client.toService(req) map { resp => () }
  }

  def deregister(hosts: String, nodeName: String, serviceId: String): Future[Unit] = {
    val payload = """{
      "Node": "$nodeName",
      "ServiceID": "$serviceId",
      "CheckID": "service:$serviceId"
    }"""
    val client = Http.newClient(hosts) // xxx: memoize
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, deregisterPath)
    req.setContent(ChannelBuffers.copiedBuffer(payload, UTF_8))
    logger.info(s"Deregister consul service $serviceId")
    client.toService(req) map { resp =>
      // xxx: debug log level
      logger.info(s"Deregister $serviceId response $resp")
      // remove process that periodically update TTL on Consul
      synchronized {
        timerTasks.get(serviceId) match {
          case Some(ttask) => ttask.close()
          case None => ()
        }
      }
      ()
    }
  }

  def announce(
    ia: InetSocketAddress,
    hosts: String,
    name: String,
    dc: Option[String]
  ): Future[Announcement] = {
    val id = randomUUID.toString
    val address = ia.getAddress.getHostAddress
    val service = ConsulService(id, name, address, ia.getPort)
    val node = ConsulNode(dc, ia.getHostName, address, service)
    register(hosts, node) map { resp =>
      new Announcement {
        def unannounce() = deregister(hosts, ia.getHostName, id)
      }
    }
  }

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    addr.split("!") match {
      // consul!hosts!/name?dc=<>&ttl=<>&health=<>&tags=<,>
      case Array(hosts, name) => announce(ia, hosts, name, None)

      // consul!host!name!datacenter
      case Array(hosts, name, dc) => announce(ia, hosts, name, Some(dc))

      case _ => {
        val exc = new ConsulAnnouncerException("Invalid addr \"%s\"".format(addr))
        Future.exception(exc)
      }
    }
  }

}
