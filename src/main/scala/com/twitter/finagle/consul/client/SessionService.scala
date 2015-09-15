package com.twitter.finagle.consul.client

import com.twitter.finagle.consul.ConsulErrors
import com.twitter.finagle.httpx.{Method, Request => HttpRequest, Response => HttpResponse}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.Future
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

class SessionService(httpClient: HttpxService[HttpRequest, HttpResponse]) {
  import SessionService._

  implicit val format = org.json4s.DefaultFormats

  def create(createRequest: CreateRequest): Future[CreateResponse] = {
    val httpRequest = HttpRequest(Method.Put, "/v1/session/create")
    httpRequest.setContentTypeJson()
    val httpBody: String = Serialization.write(createRequest)
    httpRequest.write(httpBody)
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(parse(reply.contentString).extract[CreateResponse])
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def destroy(session: String): Future[Unit] = {
    val httpRequest = HttpRequest(Method.Put, s"/v1/session/destroy/$session")
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(Unit)
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def renew(session: String): Future[Option[SessionResponse]] = {
    val httpRequest = HttpRequest(Method.Put, s"/v1/session/renew/$session")
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(Option(parse(reply.contentString).extract[SessionResponse]))
        case 404 => Future.value(None)
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def info(session: String): Future[Option[SessionResponse]] = {
    val httpRequest = HttpRequest(Method.Get, s"/v1/session/info/$session")
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(Option(parse(reply.contentString).extract[SessionResponse]))
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }
}

object SessionService {
  case class CreateRequest(LockDelay: String,  Name: String, Behavior: String, TTL: String)
  case class CreateResponse(ID: String)
  case class SessionResponse(LockDelay: Int, Checks: Set[String], Node: String, ID: String, CreateIndex: Int, Behavior: String, TTL: String)

  def apply(httpClient: HttpxService[HttpRequest, HttpResponse]) = new SessionService(httpClient)
}
