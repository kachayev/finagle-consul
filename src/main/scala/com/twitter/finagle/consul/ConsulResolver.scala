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

  type Addresses = Seq[InetSocketAddress]
  type SAddresses = Seq[SocketAddress]

  def catalogPath(name: String) = s"/v1/catalog/service/$name"

  def locationToAddr(location: ServiceLocation): InetSocketAddress = {
    val address = if("" == location.ServiceAddress) location.Address
                  else location.ServiceAddress
    new InetSocketAddress(address, location.ServicePort)
  }

  // xxx: implement datacenter option support
  // xxx: implement tags option support (filtering)
  // xxx: memoize client
  def readCatalog(hosts: String, q: ConsulQuery): Future[Addresses] = {
    val client = Http.newClient(hosts)
    val req = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, catalogPath(q.name))
    // xxx: timeout? 
    // xxx: error?
    client.toService(req) map { resp =>
      val output = new ChannelBufferInputStream(resp.getContent)
      // xxx: JSON formatting error?
      val addrs = parse(output).extract[List[ServiceLocation]] map locationToAddr
      // xxx: debug log only
      logger.info(s"Consul catalog lookup at $hosts to look for ${q.name}: $addrs")
      addrs
    }
  }

  private val timer = DefaultTimer.twitter
  private val futurePool = FuturePool.unboundedPool

  // xxx: watch changes (?)
  def addrOf(hosts: String, query: ConsulQuery): Var[Addr] =
    Var.async(Addr.Pending: Addr) { u =>
      readCatalog(hosts, query) onSuccess { (addrs: SAddresses) =>
        u() = Addr.Bound(addrs.toSet)
      }
      query.ttl match {
        case Some(ttl) =>
          // is there any reason for keeping Updater private?
          val updater = new Updater[Unit] {
            val one = Seq(())
            // just perform one update at a time
            protected def preprocess(elems: Seq[Unit]) = one
            protected def handle(unit: Unit) {
              // this is future pool, so it's ok to wait a bit
              val addrs: Seq[SocketAddress] = Await.result(readCatalog(hosts, query))
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
    // consul!host:8500!/name?dc=DC1ttl=10&tag=prod&tag=trace
    case Array(hosts, query) =>
      ConsulQuery.decodeString(query) match {
        case Some(q) => addrOf(hosts, q)
        case None =>
          throw new ConsulResolverException("Invalid address \"%s\"".format(arg))  
      }

    case _ =>
      throw new ConsulResolverException("Invalid address \"%s\"".format(arg))
  }  
}
