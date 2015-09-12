package com.twitter.finagle.consul

import com.twitter.finagle.Http
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.finagle.ServiceFactory
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}
import java.net.InetSocketAddress
import scala.collection._

object ConsulConnection {

  type Client = ServiceFactory[HttpRequest, HttpResponse]

  private[this] var clients: mutable.Map[String, Client] = mutable.Map()

  def get(hosts: String): Client = {
    var client = clients.getOrElseUpdate(hosts, {
      Http.newClient(hosts)
    })
    client
  }
}

trait ConsulConnection {
  def getClient(hosts: String) = ConsulConnection.get(hosts)
}
