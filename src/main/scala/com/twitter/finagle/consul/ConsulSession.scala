package com.twitter.finagle.consul

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.logging.{Level, Logger}

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Method, Request, Response}
import com.twitter.util.{Await, NonFatal, Return, Throw, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._

class ConsulSession(client: Service[Request, Response], opts: ConsulSession.CreateOptions) extends ConsulConstants {

  import ConsulSession._

  private[this] implicit val format = org.json4s.DefaultFormats

  val log = Logger.getLogger(getClass.getName)
  private[consul] var sessionId     = Option.empty[SessionId]
  private[this]   var heartbeat     = Option.empty[Thread]
  private[this]   var listeners     = List.empty[Listener]
  private[this]   val inChannel     = new LinkedBlockingQueue[Boolean]()

  def start(): Unit = {
    heartbeat.getOrElse {
      heartbeat = Some(spawnHeartbeat(this, opts.interval))
    }
  }

  def stop(): Unit = {
    heartbeat foreach { th =>
      inChannel.offer(true)
      th.join()
      heartbeat = None
    }
  }

  def isOpen = sessionId.isDefined

  def info(): Option[InfoReply] = {
    sessionId flatMap infoReq
  }

  def addListener(listener: Listener): Unit = {
    synchronized {
      listeners = listeners ++ List(listener)
    }
    sessionId foreach listener.start
  }

  def delListener(listener: Listener): Unit = {
    synchronized {
      listeners = listeners.filterNot(_ == listener)
    }
  }

  private[consul] def renew(): Unit = {
    sessionId map { sid =>
      val reply = renewReq(sid)
      if(!reply) {
        log.info(s"Consul session not found id=$sid")
        close()
      }
      reply
    }
  }

  private[consul] def tickListeners(): Unit = {
    sessionId foreach { sid =>
      listeners foreach { listener =>
        muted("Listener.tick", () => listener.tick(sid))
      }
    }
  }

  private[consul] def open(): Unit = {
    synchronized {
      sessionId getOrElse {
        val reply = createReq()
        log.info(s"Consul session created id=${reply.ID}")
        listeners foreach { l => muted[Unit]("Listener.call", () => l.start(reply.ID)) }
        sessionId = Some(reply.ID)
      }
    }
  }

  private[consul] def close(): Unit = {
    synchronized {
      if (sessionId.isDefined) {
        sessionId foreach { id =>
          listeners foreach { l => muted[Unit]("Listener.call", () => l.stop(id)) }
          muted("Session.destroy", () => destroyReq(id))
          log.info(s"Consul session removed id=$id")
        }
        sessionId = None
      }
    }
  }

  private[this] def muted[T](name: String, f: () => T): Try[T] = {
    Try{ f() } match {
      case Return(value) =>
        Return(value)
      case Throw(e) =>
        log.log(Level.SEVERE, e.getMessage, e)
        Throw(e)
    }
  }

  private[this] def spawnHeartbeat(me: ConsulSession, interval: Int) = new Thread("Consul Heartbeat") {
    setDaemon(true)
    start()

    override def run() {
      var running  = true
      var cooldown = false

      me.log.info("Consul heartbeat thread started")

      while(running) {
        try {

          if (!me.isOpen) {
            if (cooldown) {
              Thread.sleep(SESSION_HEARTBEAT_COOLDOWN)
            }
            cooldown = false
            me.open()
          }

          inChannel.poll(interval, TimeUnit.SECONDS) match {
            case true =>
              running = false
              me.close()
            case false if me.isOpen =>
              me.log.fine("Consul heartbeat tick")
              me.renew()
              me.tickListeners()
            case _ =>
              me.log.info(s"Consul session closed, reopen")
          }
        } catch {
          case NonFatal(e) =>
            log.log(Level.SEVERE, e.getMessage, e)
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
    reply.getStatusCode() match {
      case 200 => parse(reply.contentString).extract[CreateReply]
      case _   => throw ConsulErrors.badResponse(reply)
    }
  }

  private[this] def destroyReq(id: SessionId): Boolean = {
    val req = Request(Method.Put, SESSION_DESTROY_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    reply.getStatusCode() match {
      case 200 => true
      case 404 => false
      case e   => throw ConsulErrors.badResponse(reply)
    }
  }

  private[this] def renewReq(id: SessionId): Boolean = {
    val req = Request(Method.Put, SESSION_RENEW_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    reply.getStatusCode() match {
      case 200 => true
      case 404 => false
      case _   => throw ConsulErrors.badResponse(reply)
    }
  }

  private[this] def infoReq(id: SessionId): Option[InfoReply] = {
    val req = Request(Method.Get, SESSION_INFO_PATH.format(id))
    req.setContentTypeJson()
    val reply = Await.result(client(req))
    reply.getStatusCode() match {
      case 200 =>
        Option(parse(reply.contentString).extract[InfoReply])
      case 404 =>
        None
      case _   => throw ConsulErrors.badResponse(reply)
    }
  }
}

object ConsulSession {
  type SessionId = String

  trait Listener {
    def start(id: SessionId) : Unit
    def stop(id: SessionId) : Unit
    def tick(id: SessionId): Unit = {}
  }

  case class CreateOptions(name: String, ttl: Int = 45, interval: Int = 20, lockDelay: Int = 10)

  case class CreateReply(ID: SessionId)
  case class InfoReply(LockDelay: String, Checks: List[String], Node: String, ID: String, CreateIndex: Int)
}
