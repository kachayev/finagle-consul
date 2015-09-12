package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import com.twitter.finagle.{Httpx, ServiceFactory}
import com.twitter.finagle.httpx
import com.twitter.util.Await

class ConsulSessionSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val client  = Httpx.newService("localhost:8500")

  "open/reopen/close" in {
    val session = new ConsulSession(client, ConsulSession.CreateOptions("spec", ttl = 10, interval = 1, lockDelay = 1))

    try {
      session.start()
      Thread.sleep(5000)
      val reply0 = Await.result(session.info())
      session.close()

      Thread.sleep(5000)
      val reply1 = Await.result(session.info())
      assert(reply0.ID != reply1.ID)

    } finally {
      session.stop()
    }
  }

  "heartbeat lost" in {
    val session = new ConsulSession(client, ConsulSession.CreateOptions("spec", ttl = 10, interval = 30, lockDelay = 1))

    try {
      session.start()
      Thread.sleep(5000)
      val reply0 = Await.result(session.info())

      Thread.sleep(20000)
      val reply1 = Await.result(session.info().liftToTry)
      println(reply1)
      assert(reply1.isThrow)

    } finally {
      session.stop()
    }
  }
}
