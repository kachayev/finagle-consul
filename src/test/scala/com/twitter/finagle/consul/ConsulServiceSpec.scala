package com.twitter.finagle.consul

import com.twitter.finagle.Httpx
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulServiceSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  val client  = Httpx.newService("localhost:8500")

  "create/list/destroy" in {
    val session0 = new ConsulSession(client, ConsulSession.CreateOptions("spec"))
    val session1 = new ConsulSession(client, ConsulSession.CreateOptions("spec"))
    val service  = new ConsulService(client)

    try {
      var instances = List.empty[ConsulService.Service]

      session0.open()
      session1.open()

      val newInstance0 = ConsulService.FinagleService(session0.sessionId.get, "my/name", "example.com", 80, Set("one", "two"))
      val newInstance1 = ConsulService.FinagleService(session1.sessionId.get, "my/name", "example.com", 80, Set("one", "two"))

      assert(newInstance0.id != newInstance1.id)

      service.create(newInstance0)
      instances = service.list(newInstance0.name)
      assert(instances.length == 1)
      assert(instances.head   == newInstance0)

      service.create(newInstance1)
      instances = service.list(newInstance1.name)
      assert(instances.length == 2)
      assert(instances.map(_.id).sorted == List(newInstance0, newInstance1).map(_.id).sorted)

      service.destroy(session0.sessionId.get, newInstance0.name)

      Thread.sleep(2000)

      instances = service.list(newInstance1.name)
      assert(instances.length == 1)
      assert(instances.head   == newInstance1)

      service.destroy(session1.sessionId.get, newInstance0.name)
      instances = service.list(newInstance1.name)
      assert(instances.isEmpty)

    } finally {
      session0.stop()
      session1.stop()
    }

    Thread.sleep(1000)
  }
}
