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

class ConsulAnnouncer extends Announcer {

  import ConsulAnnouncer._

  val scheme = "consul"
  val logger = Logger.getLogger(getClass.getName)

  def sessionListener(p: ListenerParams, sid: String, connected: Boolean): Unit = {
    if (connected) {
      val newSrv = ConsulService.Service(sid, p.name, p.address, p.port, p.tags)
      p.service.create(newSrv)
    } else {
      p.service.destroy(sid, p.name)
    }
  }

  def announce(ia: InetSocketAddress, hosts: String, q: ConsulQuery): Future[Announcement] = {
    val address  = ia.getAddress.getHostAddress
    val session  = ConsulSessionFactory.getSession(hosts)
    val service  = new ConsulService(ConsulClientFactory.getClient(hosts))
    val params   = ListenerParams(service, q.name, address, ia.getPort, q.tags)
    val listener: ConsulSession.Listener = sessionListener(params, _, _)

    session.addListener(listener)
    session.start()

    Future {
      new Announcement {
        def unannounce() = Future[Unit] { session.stop() }
      }
    }
  }

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    addr.split("!") match {
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

object ConsulAnnouncer {
  case class ListenerParams(service: ConsulService, name: String, address: String, port: Int, tags: Set[String])
}
