package com.twitter.finagle.consul

import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

class ConsulQuerySpec extends WordSpecLike with Matchers with BeforeAndAfterAll {

  "parse values" in {
    ConsulQuery.decodeString("/name?dc=DC&ttl=10&tag=prod&tag=trace") match {
      case Some(ConsulQuery(name, ttl, tags, dc)) =>
        assert(name          == "name")
        assert(ttl.toString  == "Some(10.seconds)")
        assert(tags          == Set("prod", "trace", "finagle"))
        assert(dc            == Some("DC"))
    }
  }

  "parse empty" in {
    ConsulQuery.decodeString("") match {
      case Some(ConsulQuery(name, ttl, tags, dc)) =>
        assert(name          == "")
        assert(ttl           == None)
        assert(tags          == Set("finagle"))
        assert(dc            == None)
    }
  }
}
