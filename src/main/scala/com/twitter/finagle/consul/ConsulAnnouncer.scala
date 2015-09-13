package com.twitter.finagle.consul

import com.twitter.util.Future
import com.twitter.finagle.{Announcer, Announcement}
import java.net.InetSocketAddress

class ConsulAnnouncer extends Announcer {

  import ConsulAnnouncer._

  val scheme = "consul"

  // TODO: move to separate class instance
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
    session.incServices()
    session.start()

    Future {
      new Announcement {
        def unannounce() = Future[Unit] {
          session.delListener(listener)
          session.decServices()
          session.stop()
        }
      }
    }
  }

  def announce(ia: InetSocketAddress, addr: String): Future[Announcement] = {
    addr.split("!") match {
      case Array(hosts, query) =>
        ConsulQuery.decodeString(query) match {
          case Some(q) => announce(ia, hosts, q)
          case None => {
            val exc = new IllegalArgumentException(s"Invalid addr '$addr'")
            Future.exception(exc)
          }
        }

      case _ => {
        val exc = new IllegalArgumentException(s"Invalid addr '$addr'")
        Future.exception(exc)
      }
    }
  }
}

object ConsulAnnouncer {
  case class ListenerParams(service: ConsulService, name: String, address: String, port: Int, tags: Set[String])
}
