package com.twitter.finagle.consul.examples

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.handler.codec.http.HttpVersion._
import com.twitter.io.Charsets
import scala.util.Random

class RandomNumberClient(val path: String) {

  val id = Random.alphanumeric.take(10).mkString
  val client: Service[HttpRequest, HttpResponse] =
    Http.newService(path)

  def run(counter: Int): Future[Seq[HttpResponse]] =
    Future.collect((1 to 50) map ask)

  def ask(index: Int): Future[HttpResponse] = {
    val request = new DefaultHttpRequest(HTTP_1_1, HttpMethod.GET, "/")
    request.headers().add("X-Client-Id", id)
    request.headers().add("X-Req-Id", index)    

    println(s"Sending req #$index")

    val resp = client(request)
    resp onSuccess { resp: HttpResponse =>
      println(resp.getContent.toString(Charsets.Utf8))
    }
    resp
  }

}

object Client extends App {

  val path = "consul!localhost:8500!/RandomNumber?tag=prod&dc=dc1&ttl=45"
  val rn = new RandomNumberClient(path)

  println(s"Run CLIENT: ${rn.id}")

  Await.result(rn.run(50))

}
