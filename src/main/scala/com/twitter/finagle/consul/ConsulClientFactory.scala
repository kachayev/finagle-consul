package com.twitter.finagle.consul

import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.finagle.{Httpx, Service}

import scala.collection.mutable

object ConsulClientFactory {

  type Client = Service[Request, Response]

  private[this] val clients: mutable.Map[String, Client] = mutable.Map()

  def getClient(hosts: String): Client = {
    synchronized {
      val client = clients.getOrElseUpdate(hosts, {
        Httpx.newService(hosts)
      })
      client
    }
  }
}
