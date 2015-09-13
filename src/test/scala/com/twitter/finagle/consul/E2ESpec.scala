package com.twitter.finagle.consul

import com.twitter.finagle.httpx.{Method, Request, Response, Status}
import com.twitter.finagle.{Httpx, Service}
import com.twitter.util.{Await, Future}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class E2ESpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  "servers and client comunication" in {
    val service0 = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(req.version, Status.Ok))
    }

    val server0 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)
    // different consul hosts for different sessions
    val server1 = Httpx.serveAndAnnounce("consul!127.0.0.1:8500!/E2ESpec", service0)

    Thread.sleep(2000)

    val client = Httpx.newService("consul!localhost:8500!/E2ESpec?ttl=1")
    val req    = Request(Method.Get, "/")

    // live: 0,1
    Await.result(client(req))
    // live 1
    server0.close()

    Thread.sleep(2000)
    val server2 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)
    Thread.sleep(2000)

    // live 0,2
    Await.result(client(req))
    // live 2
    server1.close()

    Thread.sleep(2000)
    val server3 = Httpx.serveAndAnnounce("consul!127.0.0.1:8500!/E2ESpec", service0)
    Thread.sleep(2000)

    // live 2,3
    Await.result(client(req))
    server2.close()
    server3.close()

    client.close()

    Thread.sleep(2000)
  }
}
