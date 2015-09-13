package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import com.twitter.finagle.httpx.{Request, Response, Status, Method}
import com.twitter.finagle.{Httpx, Service}
import com.twitter.util.{Future, Await, Try}

class E2ESpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  "servers and client comunication" in {
    val service0 = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(req.version, Status.Ok))
    }

    val service1 = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(req.version, Status.Ok))
    }

    val server0 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", ":11300", service0)
    // different consul hosts for different sessions
    val server1 = Httpx.serveAndAnnounce("consul!127.0.0.1:8500!/E2ESpec", ":11301", service1)

    Thread.sleep(2000)

    val client = Httpx.newService("consul!localhost:8500!/E2ESpec?ttl=1")
    val req    = Request(Method.Get, "/")

    Await.result(client(req))
    server0.close()

    Thread.sleep(2000)
    val server2 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", ":11302", service1)
    Thread.sleep(2000)

    Await.result(client(req))
    server1.close()

    Thread.sleep(2000)
    val server3 = Httpx.serveAndAnnounce("consul!127.0.0.1:8500!/E2ESpec", ":11303", service1)
    Thread.sleep(2000)

    Await.result(client(req))
    server2.close()
    server3.close()

    client.close()

    Thread.sleep(2000)
  }
}
