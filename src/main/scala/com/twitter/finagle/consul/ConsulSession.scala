package com.twitter.finagle.consul

import com.twitter.finagle.{Httpx, Service}
import com.twitter.finagle.httpx.{ Request, Response, Method}
import java.io.{StringReader, Reader}
import com.twitter.util.{Await, NonFatal, Try, Future, Throw, Return}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import java.util.concurrent.TimeUnit

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.logging.Logger

class ConsulSession(client: Service[Request, Response], name: String, val ttl: Int = 10, val interval: Int = 5) {

  import ConsulSession._

  implicit val format = org.json4s.DefaultFormats

  val log        = Logger.getLogger(getClass.getName)
  var sessionId  = Option.empty[String]
  var heartbeat  = Option.empty[Thread]
  val inChannel  = new LinkedBlockingQueue[Boolean]()
  val outChannel = new LinkedBlockingQueue[Boolean]()

  def start() = {
    heartbeat = Some(spawnHeartbeat(this))
  }

  def stop() = {
    inChannel.offer(true)
    outChannel.take()
    heartbeat = None
  }

  def isOpen = !sessionId.isEmpty

  def info(): Future[InfoReply] = {
    sessionId match {
      case Some(id) =>
        val req = Request(Method.Get, INFO_PATH.format(id))
        req.setContentTypeJson()
        client(req) transform {
          case Return(reply) =>
            val _info = parse(reply.contentString).extract[InfoReply]
            if (_info == null) {
              Future.exception(new SessionNotExists("Consul session is not exists"))
            } else {
              Future.value(_info)
            }
          case Throw(e) =>
            Future.exception(e)
        }
      case None =>
        Future.exception(new SessionNotExists("Consul session is not exists"))
    }
  }

  private[consul] def renew() = {
    sessionId foreach renewReq
    sessionId
  }

  private[consul] def open() = {
    synchronized {
      sessionId getOrElse {
        val reply = createReq()
        log.info(s"Consul session created ${reply.ID}")
        sessionId = Some(reply.ID)
        sessionId
      }
    }
  }

  private[consul] def close() = {
    synchronized {
      sessionId foreach { id =>
        destroyReq(id)
        log.info(s"Consul session removed ${id}")
      }
      sessionId = None
    }
  }

  private[this] def spawnHeartbeat(me: ConsulSession) = new Thread("Consul Heartbeat") {
    setDaemon(true)
    start()

    override def run() {
      var running  = true
      var cooldown = false

      me.log.info(s"Consul heartbeat thread started")

      while(running) {
        try {
          if (cooldown) {
            Thread.sleep(HEARTBEAT_COOLDOWN)
            cooldown = false
          }

          if (!me.isOpen) {
            me.open()
          }

          val gone = inChannel.poll(me.interval, TimeUnit.SECONDS)
          if (gone == true) {
            running = false
            Try { me.close() }
            outChannel.offer(true)
          } else {
            println(s"${me.sessionId}")
            me.renew() foreach { id =>
              me.log.info(s"Consul heartbeat tick $id")
            }
          }
        } catch {
          case NonFatal(e) =>
            log.info(s"${e.getClass.getName}: ${e.getMessage}")
            Try{ me.close() }
            cooldown = true
        }
      }
      me.log.info(s"Consul heartbeat thread stopped")
    }
  }

  private[this] def createReq() = {
    val req  = Request(Method.Put, CREATE_PATH)
    val body = s"""{ "LockDelay": "${ttl}s", "Name": "${name}", "Behavior": "delete", "TTL": "${ttl}s" }"""
    req.write(body)
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    reply.getStatusCode match {
      case 200 => parse(reply.contentString).extract[CreateReply]
      case err => throw new InvalidResponse(s"$err: ${reply.contentString}")
    }
  }

  private[this] def destroyReq(id: String) = {
    val req = Request(Method.Put, DESTROY_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }

  private[this] def renewReq(id: String) = {
    val req = Request(Method.Put, RENEW_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }
}

object ConsulSession {
  case class CreateReply(ID: String)
  case class InfoReply(LockDelay: String, Checks: List[String], Node: String, ID: String, CreateIndex: Int)

  class SessionNotExists(msg: String) extends RuntimeException(msg)
  class InvalidResponse(msg: String) extends RuntimeException(msg)

  val HEARTBEAT_COOLDOWN = 5000

  val CREATE_PATH  = "/v1/session/create"
  val DESTROY_PATH = "/v1/session/destroy/%s"
  val RENEW_PATH   = "/v1/session/renew/%s"
  val INFO_PATH    = "/v1/session/info/%s"
}
