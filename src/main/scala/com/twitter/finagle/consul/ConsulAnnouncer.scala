package com.twitter.finagle.consul

import com.twitter.finagle.Http
import com.twitter.util.{Future, FuturePool, Time, TimerTask, Duration}
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

case class ConsulService(
  id: String,
  name: String,
  tags: Set[String],
  ttl: Option[Duration],
  address: String,
  port: Int
)

case class ConsulNode(
  dc: Option[String],
  name: String,
  address: String,
  service: ConsulService
)

class ConsulAnnouncer extends Announcer {

  val scheme = "consul"
  val registerPath = "/v1/catalog/register"
  val deregisterPath = "/v1/catalog/deregister"
  val logger = Logger.getLogger(getClass.getName)

  private val timer = DefaultTimer.twitter
  private val futurePool = FuturePool.unboundedPool
  private val timerTasks = MutableMap.empty[String, TimerTask]

  val infiniteTTL = 100000000 // ~3 years

  def escape(s: String): String = "\"" + s + "\""

  // xxx: implement custom health check(s)
  // xxx: memorize newClient operation
  // xxx: cleanup code for JSON encoding
  def send(hosts: String, node: ConsulNode)(f: HttpResponse => Boolean) = {
    val client = Http.newClient(hosts)
    val tags = node.service.tags.map(escape(_)).mkString(", ")
    val dc = node.dc.map(escape(_)).getOrElse("null")
    val payload = s"""{
      "Datacenter": $dc,
      "Node": "${node.name}",
      "Address": "${node.address}",
      "Service": {
        "ID": "${node.service.id}",
        "Tags": [$tags],
        "Service": "${node.service.name}",
        "Address": "${node.service.address}",
        "Port": ${node.service.port}
      },
      "Check": {
        "Node": "${node.name}",
        "CheckID": "service:${node.service.id}",
        "Status": "passing",
        "ServiceID": "${node.service.id}",
        "Ttl": "${node.service.ttl.getOrElse(infiniteTTL)}s"
      }
    }"""

    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, registerPath)
    req.setContent(ChannelBuffers.copiedBuffer(payload, UTF_8))
    req.headers().add("Host", "localhost:8500")
    client.toService(req) map f
  }

  def register(hosts: String, node: ConsulNode): Future[Boolean] = {
    send(hosts, node) { resp =>
      logger.info(s"Register consul service ${node.service}: $resp")
      val wasSuccessful = resp.getStatus.getCode == 200
      if(wasSuccessful) {
        node.service.ttl match {
          case None => ()
          case Some(ttl) => {
            val ttask = timer.schedule(ttl.fromNow, ttl) {
              // reregister service to deal with health-check TTL
              futurePool(prolongate(hosts, node))
            }
            synchronized {
              timerTasks += (node.service.id -> ttask)
            }
          }
        }
      }
      wasSuccessful
    }
  }

  def prolongate(hosts: String, node: ConsulNode): Future[Boolean] = {
    send(hosts, node) { resp =>
      // xxx: debug log only
      logger.info(s"Prolongate consul service ${node.service}: $resp")
      resp.getStatus.getCode == 200
    }
  }

  // xxx: memoize newClient operation
  def deregister(hosts: String, nodeName: String, serviceId: String): Future[Unit] = {
    val payload = """{
      "Node": "$nodeName",
      "ServiceID": "$serviceId",
      "CheckID": "service:$serviceId"
    }"""
    val client = Http.newClient(hosts) 
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, deregisterPath)
    req.setContent(ChannelBuffers.copiedBuffer(payload, UTF_8))
    client.toService(req) map { resp =>
      // xxx: debug log level
      logger.info(s"Deregister consul service $serviceId: $resp")
      // remove task (process?) that periodically update TTL on Consul
      synchronized {
        timerTasks.get(serviceId) match {
          case Some(ttask) => ttask.close()
          case None => ()
        }
      }
      ()
    }
  }

  def announce(ia: InetSocketAddress, hosts: String, q: ConsulQuery): Future[Announcement] = {
    val id = randomUUID.toString
    val address = ia.getAddress.getHostAddress
    val service = ConsulService(id, q.name, q.tags, q.ttl, address, ia.getPort)
    val node = ConsulNode(q.dc, ia.getHostName, address, service)
    register(hosts, node) map { resp =>
      new Announcement {
        def unannounce() = deregister(hosts, ia.getHostName, id)
      }
    }
  }

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    addr.split("!") match {
      // consul!host1:port1,host2:port2!/name?dc=DC1&ttl=100&tag=finalge&tag=prod
      case Array(hosts, query) =>
        ConsulQuery.decodeString(query) match {
          case Some(q) => announce(ia, hosts, q)
          case None => {
            val exc = new ConsulAnnouncerException(s"Invalid addr '$addr'")
            Future.exception(exc)
          }
        }

      case _ => {
        val exc = new ConsulAnnouncerException(s"Invalid addr '$addr'")
        Future.exception(exc)
      }
    }
  }

}
