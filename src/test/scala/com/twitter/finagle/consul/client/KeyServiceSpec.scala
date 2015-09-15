package com.twitter.finagle.consul.client

import com.twitter.finagle.{Httpx, httpx}
import com.twitter.util.Await
import org.scalatest.WordSpec
import org.json4s.jackson.JsonMethods._

class KeyServiceSpec extends WordSpec {

  case class Value(name: String)
  case class Session(ID: String)

  val httpClient = Httpx.newService("localhost:8500")
  val service    = KeyService(httpClient)

  "simple create/get/destroy" in {
    val path       = "test/key0"
    val value      = Value("test")

    val Some(createReply) = Await.result(service.putJson[Value](path, value))
    assert(createReply.Session.isEmpty)
    assert(createReply.Key == path)
    assert(createReply.Value.toString == "Value(test)")

    val Some(getReply) = Await.result(service.getJson[Value](path))
    assert(getReply.Session.isEmpty)
    assert(getReply.Key == path)
    assert(getReply.Value.toString == "Value(test)")

    Await.result(service.delete(path))

    val allReply = Await.result(service.getJsonSet[Value](path))
    assert(allReply.isEmpty)
  }

  "find recursive" in {
    val path0 = "test/key/0"
    val path1 = "test/key/1"
    val path2 = "test/key/1/2"
    val value0 = Value("test0")
    val value1 = Value("test1")
    val value2 = Value("test2")

    val Some(_) = Await.result(service.putJson[Value](path0, value0))
    val Some(_) = Await.result(service.putJson[Value](path1, value1))
    val Some(_) = Await.result(service.putJson[Value](path2, value2))

    val allReply = Await.result(service.getJsonSet[Value]("test/key"))
    assert(allReply.size == 3)
    assert(allReply.map(_.Key)   == Set("test/key/0", "test/key/1/2", "test/key/1"))
    assert(allReply.map(_.Value.name).toString == "Set(test0, test2, test1)")

    Await.result(service.delete(path0))
    Await.result(service.delete(path1))
    Await.result(service.delete(path2))
  }

  "acquire/release" in {
    implicit val format = org.json4s.DefaultFormats

    val lock  = "test/lock0"
    val value = Value("test")
    val body  = s"""{ "LockDelay": "10s", "Name": "test", "Behavior": "delete", "TTL": "10s" }"""
    val createSession = httpx.Request(httpx.Method.Put, "/v1/session/create")
    createSession.write(body)

    val sessionReply0 = Await.result(httpClient(createSession))
    val sessionReply1 = Await.result(httpClient(createSession))

    val session0 = parse(sessionReply0.contentString).extract[Session]
    val session1 = parse(sessionReply1.contentString).extract[Session]

    assert(Await.result(service.acquireJson[Value](lock, value, session0.ID)))

    assert(!Await.result(service.acquireJson[Value](lock, value, session0.ID)))
    assert(!Await.result(service.acquireJson[Value](lock, value, session1.ID)))

    assert(Await.result(service.getJson[Value](lock)).head.Session.head == session0.ID)

    assert(Await.result(service.releaseJson[Value](lock, value, session0.ID)))

    assert(Await.result(service.getJson[Value](lock)).head.Session.isEmpty)
    assert(Await.result(service.getJson[Value](lock)).head.Value.name == value.name)

    service.delete(lock)
  }

}
