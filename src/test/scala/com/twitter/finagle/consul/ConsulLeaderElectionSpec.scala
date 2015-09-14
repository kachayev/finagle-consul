package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulLeaderElectionSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  import ConsulLeaderElection.LeaderElectionStatus._

  "leader election" in {
    val session0 = ConsulSessionFactory.getSession("localhost:8500")
    val session1 = ConsulSessionFactory.getSession("127.0.0.1:8500")
    val client   = ConsulClientFactory.getClient("localhost:8500")
    val leader0  = new ConsulLeaderElection("spec", client, session0)
    val leader1  = new ConsulLeaderElection("spec", client, session1)

    try {
      leader0.start()
      Thread.sleep(1000)
      assert(leader0.getStatus == Leader)

      leader1.start()
      Thread.sleep(12000)
      assert(leader1.getStatus == Pending)

      leader0.stop()
      Thread.sleep(12000)
      assert(leader1.getStatus == Leader)

      leader0.start()
      Thread.sleep(1000)
      assert(leader0.getStatus == Pending)
    } finally {
      leader0.stop()
      leader1.stop()
    }
  }
}
