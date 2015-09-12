package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulQuerySpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "parse values" in {
    ConsulQuery.decodeString("/name?dc=DC&ttl=45&tag=prod&tag=trace") match {
      case Some(ConsulQuery(name, ttl, tags, dc)) =>
        assert(name          == "name")
        assert(ttl.toString  == "45.seconds")
        assert(tags          == Set("prod", "trace"))
        assert(dc            == Some("DC"))
    }
  }

  "parse empty" in {
    ConsulQuery.decodeString("") match {
      case Some(ConsulQuery(name, ttl, tags, dc)) =>
        assert(name          == "")
        assert(ttl.toString  == "10.seconds")
        assert(tags          == Set())
        assert(dc            == None)
    }
  }
}
