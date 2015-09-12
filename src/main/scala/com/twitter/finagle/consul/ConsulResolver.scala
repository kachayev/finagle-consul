package com.twitter.finagle.consul

import com.twitter.finagle.{Resolver, Addr}
import java.net.{InetSocketAddress, SocketAddress}
import com.twitter.util.{Var, FuturePool}
import com.twitter.util.Closable
import com.twitter.finagle.util.{Updater, DefaultTimer}

class ConsulResolver extends Resolver {
  val scheme = "consul"

  private[this] val timer      = DefaultTimer.twitter
  private[this] val futurePool = FuturePool.unboundedPool

  private[this] def addresses(hosts: String, query: ConsulQuery) : Set[SocketAddress] = {
    val services = ConsulServiceFactory.getService(hosts)
    val addrs    = services.list(query.name) map { s =>
      new InetSocketAddress(s.address, s.port)
    }
    addrs.toSet
  }

  def addrOf(hosts: String, query: ConsulQuery): Var[Addr] =
    Var.async(Addr.Pending: Addr) { u =>
      u() = Addr.Bound(addresses(hosts, query))
      val updater = new Updater[Unit] {
        val one = Seq(())
        protected def preprocess(elems: Seq[Unit]) = one
        protected def handle(unit: Unit) {
          val addrs = addresses(hosts, query)
          u() = Addr.Bound(addrs.toSet)
        }
      }
      timer.schedule(query.ttl.fromNow, query.ttl) {
        futurePool(updater(()))
      }
    }

  def bind(arg: String): Var[Addr] = arg.split("!") match {
    case Array(hosts, query) =>
      ConsulQuery.decodeString(query) match {
        case Some(q) => addrOf(hosts, q)
        case None =>
          throw new IllegalArgumentException(s"Invalid address '$arg'")
      }

    case _ =>
      throw new IllegalArgumentException(s"Invalid address '$arg'")
  }
}
