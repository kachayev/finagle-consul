package com.twitter.finagle.consul

import java.net.InetSocketAddress

import com.twitter.finagle.{Announcement, Announcer}
import com.twitter.util.Future

class ConsulAnnouncer extends Announcer {

  import ConsulAnnouncer._

  val scheme = "consul"

  def announce(ia: InetSocketAddress, hosts: String, q: ConsulQuery): Future[Announcement] = {
    val address  = ia.getAddress.getHostAddress
    val session  = ConsulSession.get(hosts)
    val service  = new ConsulService(ConsulHttpClientFactory.getClient(hosts))
    val listener = new SessionListener(service, q.name, address, ia.getPort, q.tags)

    session.addListener(listener)
    session.start()

    Future {
      new Announcement {
        def unannounce() = Future[Unit] {
          session.delListener(listener)
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
          case None =>
            val exc = new IllegalArgumentException(s"Invalid addr '$addr'")
            Future.exception(exc)
        }
      case _ =>
        val exc = new IllegalArgumentException(s"Invalid addr '$addr'")
        Future.exception(exc)
    }
  }
}

object ConsulAnnouncer {
  class SessionListener(service: ConsulService, name: String, address: String, port: Int, tags: Set[String])
    extends ConsulSession.Listener {

    def start(session: String): Unit = {
      val newSrv = ConsulService.Service(session, name, address, port, tags)
      service.create(newSrv)
    }

    def stop(session: String): Unit = {
      service.destroy(session, name)
    }
  }
}
