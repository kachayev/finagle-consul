package com.twitter.finagle.consul

import com.twitter.finagle.httpx.{Method, Request, Response, Status}
import com.twitter.finagle.{ListeningServer, Httpx, Service}
import com.twitter.util.{Await, Future}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class E2ESpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "servers and client communication" in {

    val service0 = new Service[Request, Response] {
      def apply(req: Request) = Future.value(Response(req.version, Status.Ok))
    }

    var server0: ListeningServer = null
    var server1: ListeningServer = null
    var server2: ListeningServer = null
    var server3: ListeningServer = null
    var client:  Service[Request, Response] = null

    try {
      server0 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)
      server1 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)

      Thread.sleep(2000)

      client = Httpx.newService("consul!localhost:8500!/E2ESpec?ttl=1")
      val req = Request(Method.Get, "/")

      // live: 0,1
      Await.result(client(req))
      // live 1
      server0.close()

      Thread.sleep(2000)
      server2 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)
      Thread.sleep(2000)

      // live 0,2
      Await.result(client(req))
      // live 2
      server1.close()

      Thread.sleep(2000)
      server3 = Httpx.serveAndAnnounce("consul!localhost:8500!/E2ESpec", service0)
      Thread.sleep(2000)

      // live 2,3
      Await.result(client(req))
      Thread.sleep(1000)
    } finally {
      if (server0 != null) server0.close()
      if (server1 != null) server1.close()
      if (server2 != null) server2.close()
      if (server3 != null) server3.close()
    }
  }
}
