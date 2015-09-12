package com.twitter.finagle.consul

import java.net.InetSocketAddress
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import com.twitter.util.Await

class ConsulAnnouncerSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val announcer = new ConsulAnnouncer()

  "announce success" in {
    val addr   = "localhost:8500!/name"
    val ia     = new InetSocketAddress("127.0.0.1", 3000)
    val future = announcer.announce(ia, addr)
    val reply  = Await.result(future)
    reply.unannounce()
  }

  "announce failure" in {
    val addr   = "localhost:18500!/name"
    val ia     = new InetSocketAddress("127.0.0.1", 3000)
    val future = announcer.announce(ia, addr)
    val reply  = Await.result(future.liftToTry)
    assert(reply.isThrow)
  }
}
