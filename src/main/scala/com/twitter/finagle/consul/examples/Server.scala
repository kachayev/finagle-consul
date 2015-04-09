package com.twitter.finagle.consul.examples

import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.Response
import com.twitter.util.{Await, Future}
import org.jboss.netty.handler.codec.http._
import scala.util.Random

class RandomNumberServer {

  val serverId = Random.alphanumeric.take(10).mkString

  val service = new Service[HttpRequest, HttpResponse] {
    def apply(req: HttpRequest): Future[HttpResponse] = {
      val reqId = req.getHeader("X-Req-Id")
      val clientId = req.getHeader("X-Client-Id")
      val respId = Random.alphanumeric.take(5).mkString
      val content = s"Server:$serverId; Client:$clientId; Req:$reqId; Resp:$respId"
      val response = Response()
      response.setContentString(content)
      Future.value(response)
    }
  }  

}

object Server extends App {

  val path = "consul!127.0.0.1:8500!RandomNumber"
  val rn = new RandomNumberServer

  println(s"Run server: ${rn.serverId}")

  val server = Http.serveAndAnnounce(path, rn.service)
  Await.ready(server)

}
