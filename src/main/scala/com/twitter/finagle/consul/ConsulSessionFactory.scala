package com.twitter.finagle.consul

import scala.collection.mutable

object ConsulSessionFactory {
  private[this] val sessions = mutable.Map[(String, ConsulSession.CreateOptions), ConsulSession]()

  val defaultSessionOptions = ConsulSession.CreateOptions(name = "finagle.default")

  def getSession(hosts: String, opts: ConsulSession.CreateOptions): ConsulSession = {
    synchronized {
      sessions getOrElseUpdate((hosts, opts), {
        val newSession = new ConsulSession(
          ConsulClientFactory.getClient(hosts),
          opts
        )
        newSession
      })
    }
  }

  def getSession(hosts: String): ConsulSession = getSession(hosts, defaultSessionOptions)
}
