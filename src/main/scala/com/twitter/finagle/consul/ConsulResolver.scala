package com.twitter.finagle.consul

import com.twitter.finagle.Http
import com.twitter.finagle.{Resolver, Addr}
import com.twitter.util._
import com.twitter.finagle.util._
import com.twitter.conversions.time._
import com.twitter.finagle.util.DefaultTimer

import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.buffer.ChannelBufferInputStream
import java.nio.charset.StandardCharsets._
import java.net.{SocketAddress, InetSocketAddress}
import java.util.concurrent.TimeUnit.SECONDS
import java.util.logging.Logger

import org.json4s._
import org.json4s.jackson.JsonMethods._

class ConsulResolverException(msg: String) extends Exception(msg)

case class ServiceLocation(
  Node: String,
  Address: String,
  ServiceID: String,
  ServiceName: String,
  ServiceTags: List[String],
  ServiceAddress: String,
  ServicePort: Int
)

class ConsulResolver extends Resolver {
  val scheme = "consul"
  implicit val format = org.json4s.DefaultFormats
  val logger = Logger.getLogger(getClass.getName)

  def catalogPath(name: String) = s"/v1/catalog/service/$name"

  def locationToAddr(location: ServiceLocation): InetSocketAddress = {
    val address = if("" == location.ServiceAddress) location.Address
                  else location.ServiceAddress
    new InetSocketAddress(address, location.ServicePort)
  }

  // xxx: implement datacenter option support
  // xxx: implement multiple hosts
  def readCatalog(hosts: String, name: String): Future[List[InetSocketAddress]] = {
    val client = Http.newClient(hosts) // xxx: memoize
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.PUT, catalogPath(name))
    // xxx: timeout? 
    // xxx: error?
    client.toService(req) map { resp =>
      val output = new ChannelBufferInputStream(resp.getContent)
      val addrs = parse(output).extract[List[ServiceLocation]] map locationToAddr
      // xxx: debug log only
      logger.info(s"Consul catalog lookup at $hosts to look for $name: $addrs")
      addrs
    }
  }

  private val timer = DefaultTimer.twitter
  private val futurePool = FuturePool.unboundedPool

  // xxx: in-memory cache
  // xxx: watch changes
  def addrOf(hosts: String, name: String, ttlOption: Option[Duration]): Var[Addr] =
    Var.async(Addr.Pending: Addr) { u =>
      readCatalog(hosts, name) onSuccess { (addrs: Seq[SocketAddress]) =>
        u() = Addr.Bound(addrs.toSet)
      }
      ttlOption match {
        case Some(ttl) =>
          val updater = new Updater[Unit] {
            val one = Seq(())
            // Just perform one update at a time.
            protected def preprocess(elems: Seq[Unit]) = one
            protected def handle(unit: Unit) {
              // this is future pool, so it's ok to wait a bit
              val addrs: Seq[SocketAddress] = Await.result(readCatalog(hosts, name))
              u() = Addr.Bound(addrs.toSet)
            }
          }
          timer.schedule(ttl.fromNow, ttl) {
            futurePool(updater())
          }
        case None =>
          Closable.nop
      }
    }

  def bind(arg: String): Var[Addr] = arg.split("!") match {
    // consul!host:8500!/name?dc=&ttl=&tags=&
    case Array(hosts, name) =>
      addrOf(hosts, name, Some(10.seconds))

    // consul!host:8500!name|datacenter
    case Array(hosts, name, dc) =>
      addrOf(hosts, name, None)

    case _ =>
      throw new ConsulResolverException("Invalid address \"%s\"".format(arg))
  }  
}
