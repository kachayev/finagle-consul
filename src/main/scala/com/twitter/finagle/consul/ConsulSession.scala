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

class ConsulSession(client: Service[Request, Response], opts: ConsulSession.CreateOptions) extends ConsulConstants {

  import ConsulSession._

  implicit val format = org.json4s.DefaultFormats

  val log        = Logger.getLogger(getClass.getName)
  var sessionId  = Option.empty[String]
  var heartbeat  = Option.empty[Thread]
  val inChannel  = new LinkedBlockingQueue[Boolean]()
  val outChannel = new LinkedBlockingQueue[Boolean]()

  def start(): Unit = {
    heartbeat.getOrElse {
      heartbeat = Some(spawnHeartbeat(this, opts.interval))
    }
  }

  def stop(): Unit = {
    heartbeat foreach { _ =>
      inChannel.offer(true)
      outChannel.take()
    }
    heartbeat = None
  }

  def isOpen = !sessionId.isEmpty

  def info(): Future[InfoReply] = {
    sessionId match {
      case Some(id) => infoReq(id)
      case None     => Future.exception(sessionNotFoundError)
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

  private[this] def spawnHeartbeat(me: ConsulSession, interval: Int) = new Thread("Consul Heartbeat") {
    setDaemon(true)
    start()

    override def run() {
      var running  = true
      var cooldown = false

      me.log.info(s"Consul heartbeat thread started")

      while(running) {
        try {
          if (cooldown) {
            Thread.sleep(SESSION_HEARTBEAT_COOLDOWN)
            cooldown = false
          }

          if (!me.isOpen) {
            me.open()
          }

          val gone = inChannel.poll(interval, TimeUnit.SECONDS)
          if (gone == true) {
            running = false
            Try { me.close() }
            outChannel.offer(true)
          } else {
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
    val req  = Request(Method.Put, SESSION_CREATE_PATH)
    val body = s"""{ "LockDelay": "${opts.lockDelay}s", "Name": "${opts.name}", "Behavior": "delete", "TTL": "${opts.ttl}s" }"""
    req.write(body)
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    reply.getStatusCode match {
      case 200 => parse(reply.contentString).extract[CreateReply]
      case err => throw new InvalidResponse(s"$err: ${reply.contentString}")
    }
  }

  private[this] def destroyReq(id: String) = {
    val req = Request(Method.Put, SESSION_DESTROY_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }

  private[this] def renewReq(id: String) = {
    val req = Request(Method.Put, SESSION_RENEW_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    if (reply.getStatusCode != 200) {
      throw new InvalidResponse(s"${reply.getStatusCode}: ${reply.contentString}")
    }
  }

  private[this] def infoReq(id: String): Future[InfoReply] = {
    val req = Request(Method.Get, SESSION_INFO_PATH.format(id))
    req.setContentTypeJson()
    client(req) transform {
      case Return(reply) =>
        val _info = parse(reply.contentString).extract[InfoReply]
        if (_info == null) {
          Future.exception(sessionNotFoundError(id))
        } else {
          Future.value(_info)
        }
      case Throw(e) =>
        Future.exception(e)
    }
  }

  private[this] def sessionNotFoundError(id: String) = new SessionNotFound(s"Consul session $id is not exists")
  private[this] def sessionNotFoundError() = new SessionNotFound(s"Consul session is not exists")
}

object ConsulSession {
  case class CreateOptions(name: String, ttl: Int = 10, interval: Int = 10, lockDelay: Int = 10)

  case class CreateReply(ID: String)
  case class InfoReply(LockDelay: String, Checks: List[String], Node: String, ID: String, CreateIndex: Int)

  class SessionNotFound(msg: String) extends RuntimeException(msg)
  class InvalidResponse(msg: String) extends RuntimeException(msg)


}
