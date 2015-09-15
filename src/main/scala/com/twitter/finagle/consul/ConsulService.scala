package com.twitter.finagle.consul

import java.util.logging.Logger

import com.twitter.finagle.consul.client.KeyService
import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.Await

class ConsulService(httpClient: HttpxService[Request, Response]) {

  import ConsulService._

  private val log    = Logger.getLogger(getClass.getName)
  private val client = KeyService(httpClient)

  def list(name: String): List[Service] = {
    val reply = Await.result(client.getJsonSet[Service](lockName(name)))
    reply.map(_.Value).toList
  }

  private[consul] def create(service: Service): Unit = {
    val reply = client.acquireJson[Service](lockName(service.ID, service.Service), service, service.ID)
    Await.result(reply)
    log.info(s"Consul service registered name=${service.Service} session=${service.ID} addr=${service.Address}:${service.Port}")
  }

  private[consul] def destroy(session: String, name: String): Unit = {
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
  case class Service(ID: String, Service: String, Address: String, Port: Int, Tags: Set[String], dc: Option[String] = None)
}
