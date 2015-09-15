package com.twitter.finagle.consul

import java.util.Base64

import com.twitter.finagle.httpx.{Method, Request, Response}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.{Future, Await}
import java.util.logging.Logger
import client.KeyService

class ConsulService(httpClient: HttpxService[Request, Response]) extends ConsulConstants {

  import ConsulService._

  private val log    = Logger.getLogger(getClass.getName)
  private val client = KeyService(httpClient)

  def list(name: String): List[Service] = {
    val reply = Await.result(client.getJsonSet[FinagleService](lockName(name)))
    reply.map(_.Value).toList
  }

  private[consul] def create(service: FinagleService): Unit = {
    val reply = client.acquireJson[FinagleService](lockName(service.id, service.name), service, service.id)
    Await.result(reply)
    log.info(s"Consul service registered name=${service.name} session=${service.id} addr=${service.address}:${service.port}")
  }

  private[consul] def destroy(session: ConsulSession.SessionId, name: String): Unit = {
    val reply = client.delete(lockName(session, name))
    Await.result(reply)
    log.info(s"Consul service deregistered name=$name session=$session")
  }

  private def lockName(name: String): String = {
    s"finagle/services/$name"
  }

  private def lockName(session: String, name: String): String = {
    lockName(name) + s"/$session"
  }
}

object ConsulService {

  sealed trait Service {
    val id:      String
    val name:    String
    val address: String
    val port:    Int
    val tags:    Set[String]
  }

  case class FinagleService(id: ConsulSession.SessionId, name: String, address: String, port: Int, tags: Set[String], dc: Option[String] = None, endpoints: Set[String] = Set.empty)
    extends Service
}
