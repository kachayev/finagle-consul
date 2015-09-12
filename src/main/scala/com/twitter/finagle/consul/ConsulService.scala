package com.twitter.finagle.consul

import com.twitter.finagle.{Httpx, Service => HttpxService}
import com.twitter.finagle.httpx.{ Request, Response, Method}
import com.twitter.util.{Await}

import org.json4s
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import java.util.Base64

class ConsulService(client: HttpxService[Request, Response]) extends ConsulConstants {

  import ConsulService._

  implicit val format = org.json4s.DefaultFormats

  def create(service: Service): Unit = {
    val req = Request(Method.Put, SERVICE_CREATE_PATH.format(service.name, service.sessionId, service.sessionId))
    req.setContentTypeJson()
    req.write(Serialization.write(service))
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }

  def destroy(sessionId: ConsulSession.SessionId, name: String): Unit = {
    val req = Request(Method.Delete, SERVICE_DESTROY_PATH.format(name, sessionId))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }

  def list(name: String): List[Service] = {
    val req = Request(Method.Get, SERVICE_LIST_PATH.format(name))
    req.setContentTypeJson()

    val reply = Await.result(client(req))

    if (reply.getStatusCode == 404) {
      return List.empty
    } else if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }

    val services = parse(reply.contentString).extract[List[GetReply]] map { s =>
      replyToService(s)
    }
    services
  }

  private[this] def replyToService(reply: GetReply): Service = {
    val data = new String(Base64.getDecoder().decode(reply.Value))
    parse(data).extract[Service]
  }
}

object ConsulService {
  case class Service(sessionId: ConsulSession.SessionId, name: String, address: String, port: Int, tags: List[String])
  case class GetReply(CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: String)

  class InvalidResponse(msg: String) extends RuntimeException(msg)
}
