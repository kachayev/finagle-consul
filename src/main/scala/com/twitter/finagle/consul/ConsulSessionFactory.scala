package com.twitter.finagle.consul

import scala.collection.mutable

object ConsulSessionFactory {
  val defaultSessionOptions = ConsulSession.CreateOptions(name = "finagle.default")

  def getSession(hosts: String, opts: ConsulSession.CreateOptions): ConsulSession = {
    val newSession = new ConsulSession(
      ConsulClientFactory.getClient(hosts),
      opts
    )
    newSession
  }

  def getSession(hosts: String): ConsulSession = getSession(hosts, defaultSessionOptions)
}
