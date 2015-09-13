package com.twitter.finagle.consul

import java.util.Base64

import com.twitter.finagle.httpx.{Method, Request, Response}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.Await
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import java.util.logging.Logger

class ConsulService(client: HttpxService[Request, Response]) extends ConsulConstants {

  import ConsulService._

  private[this] implicit val format = org.json4s.DefaultFormats
  private[this] val log = Logger.getLogger(getClass.getName)

  private[consul] def create(service: Service): Unit = {
    val req = Request(Method.Put, SERVICE_CREATE_PATH.format(service.name, service.sessionId, service.sessionId))
    req.setContentTypeJson()
    req.write(Serialization.write(service))
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw ConsulErrors.badResponse(reply)
    }
    log.info(s"Consul service created name=${service.name} session=${service.sessionId} addr=${service.address}:${service.port}")
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

  private[consul] def list(name: String): List[Service] = {
    val req = Request(Method.Get, SERVICE_LIST_PATH.format(name))
    req.setContentTypeJson()

    val reply = Await.result(client(req))
    reply.getStatusCode() match {
      case 200 =>
        parse(reply.contentString).extract[List[GetReply]] map replyToService
      case 404 =>
        List.empty
      case _ =>
        throw ConsulErrors.badResponse(reply)
    }
  }

  private[this] def replyToService(reply: GetReply): Service = {
    val data = new String(Base64.getDecoder.decode(reply.Value))
    parse(data).extract[Service]
  }
}

object ConsulService {
  case class Service(sessionId: ConsulSession.SessionId, name: String, address: String, port: Int, tags: Set[String])
  case class GetReply(CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: String)
}
