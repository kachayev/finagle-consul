package com.twitter.finagle.consul

object ConsulSessionFactory {
  val defaultSessionOptions = ConsulSession.Options(name = "finagle.default")

  def getSession(hosts: String, opts: ConsulSession.Options): ConsulSession = {
    val newSession = new ConsulSession(
      ConsulClientFactory.getClient(hosts),
      opts
    )
    newSession
  }

  def getSession(hosts: String): ConsulSession = getSession(hosts, defaultSessionOptions)
}
