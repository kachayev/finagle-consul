package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulLeaderElectionSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  import ConsulLeaderElection.Status

  "leader election" in {
    val opts     = ConsulSession.Options(name = "test", ttl = 10, interval = 1, lockDelay = 10)
    val session0 = ConsulSessionFactory.getSession("localhost:8500", opts)
    val session1 = ConsulSessionFactory.getSession("localhost:8500", opts)
    val client   = ConsulClientFactory.getClient("localhost:8500")
    val leader0  = new ConsulLeaderElection("spec", client, session0)
    val leader1  = new ConsulLeaderElection("spec", client, session1)

    try {
      leader0.start()
      Thread.sleep(2000)
      assert(leader0.getStatus == Status.Leader)

      leader1.start()
      Thread.sleep(2000)
      assert(leader1.getStatus == Status.Pending)

      leader0.stop()
      Thread.sleep(2000)
      assert(leader1.getStatus == Status.Leader)

      leader0.start()
      Thread.sleep(2000)
      assert(leader0.getStatus == Status.Pending)
    } finally {
      leader0.stop()
      leader1.stop()
    }
  }
}
