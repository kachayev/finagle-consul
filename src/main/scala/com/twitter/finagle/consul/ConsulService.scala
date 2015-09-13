package com.twitter.finagle.consul

import java.util.Base64

import com.twitter.finagle.httpx.{Method, Request, Response}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.{Future, Await}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import java.util.logging.Logger

class ConsulService(client: HttpxService[Request, Response]) extends ConsulConstants {

  import ConsulService._

  private[this] implicit val format = org.json4s.DefaultFormats
  private[this] val log = Logger.getLogger(getClass.getName)

  def list(name: String): List[Service] = {
    val finagleServices = getFinagleServices(name)
    Await.result(finagleServices)
  }

  private[consul] def create(service: FinagleService): Unit = {
    val req = Request(Method.Put, SERVICE_CREATE_PATH.format(service.name, service.id, service.id))
    req.setContentTypeJson()
    req.write(Serialization.write(service))
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw ConsulErrors.badResponse(reply)
    }
    log.info(s"Consul service created name=${service.name} session=${service.id} addr=${service.address}:${service.port}")
  }

  private[consul] def destroy(sessionId: ConsulSession.SessionId, name: String): Unit = {
    val req = Request(Method.Delete, SERVICE_DESTROY_PATH.format(name, sessionId))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw ConsulErrors.badResponse(reply)
    }
    log.info(s"Consul service removed name=$name session=$sessionId")
  }

  private def replyToFinagleService(reply: FinagleServiceReply): FinagleService = {
    val encodedService = new String(Base64.getDecoder.decode(reply.Value))
    parse(encodedService).extract[FinagleService]
  }

  private def getFinagleServices(name: String): Future[List[FinagleService]] = {
    val req = Request(Method.Get, SERVICE_LIST_PATH.format(name))
    req.setContentTypeJson()

    client(req) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 =>
          val srv = parse(reply.contentString).extract[List[FinagleServiceReply]] map replyToFinagleService
          Future.value(srv)
        case 404 =>
          Future.value(List.empty)
        case _ =>
          Future.exception(ConsulErrors.badResponse(reply))
      }
    }
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

  case class FinagleService(id: ConsulSession.SessionId, name: String, address: String, port: Int, tags: Set[String])
    extends Service

  case class FinagleServiceReply(CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: String)
}
