package com.twitter.finagle.consul

import com.twitter.finagle.Httpx
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulSessionSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val client  = Httpx.newService("localhost:8500")

  "open/reopen/close" in {
    val session = new ConsulSession(client, ConsulSession.CreateOptions("spec", ttl = 10, interval = 1, lockDelay = 1))

    try {
      session.start()
      Thread.sleep(5000)
      val Some(reply0) = session.info()
      session.close()

      Thread.sleep(5000)
      val Some(reply1) = session.info()
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
      assert(session.info().isDefined)

      Thread.sleep(20000)
      assert(session.info().isEmpty)

    } finally {
      session.stop()
    }
  }
}
