package com.twitter.finagle.consul

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.logging.{Level, Logger}

import com.twitter.finagle.Service
import com.twitter.finagle.consul.client.SessionService
import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.util.{Await, NonFatal, Return, Throw, Try}

class ConsulSession(httpClient: Service[Request, Response], opts: ConsulSession.Options) {

  import ConsulSession._

  val log = Logger.getLogger(getClass.getName)
  private[consul] var sessionId     = Option.empty[String]
  private[this]   var heartbeat     = Option.empty[Thread]
  private[this]   var listeners     = List.empty[Listener]
  private[this]   val inChannel     = new LinkedBlockingQueue[Boolean]()
  private[this]   val client        = SessionService(httpClient)

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

  def info(): Option[SessionService.SessionResponse] = {
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
    sessionId foreach { sid =>
      val reply = renewReq(sid)
      if(reply.isEmpty) {
        log.info(s"Consul session not found id=$sid")
        close()
      }
    }
  }

  private[consul] def tickListeners(): Unit = {
    sessionId foreach { sid =>
      listeners foreach { listener =>
        muted(() => listener.tick(sid))
      }
    }
  }

  private[consul] def open(): Unit = {
    synchronized {
      sessionId getOrElse {
        val reply = createReq()
        log.info(s"Consul session created id=${reply.ID}")
        listeners foreach { l => muted(() => l.start(reply.ID)) }
        sessionId = Some(reply.ID)
      }
    }
  }

  private[consul] def close(): Unit = {
    synchronized {
      if (sessionId.isDefined) {
        sessionId foreach { id =>
          listeners foreach { l => muted(() => l.stop(id)) }
          muted(() => destroyReq(id))
          log.info(s"Consul session removed id=$id")
        }
        sessionId = None
      }
    }
  }

  private[this] def muted[T](f: () => T): Try[T] = {
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
    val createRequest = SessionService.CreateRequest(
      LockDelay = s"${opts.lockDelay}s", Name = opts.name, Behavior = "delete", TTL = s"${opts.ttl}s"
    )
    Await.result(client.create(createRequest))
  }

  private[this] def destroyReq(session: String): Unit = {
    Await.result(client.destroy(session))
  }

  private[this] def renewReq(session: String): Option[SessionService.SessionResponse] = {
    Await.result(client.renew(session))
  }

  private[this] def infoReq(session: String): Option[SessionService.SessionResponse] = {
    Await.result(client.info(session))
  }
}

object ConsulSession {

  val SESSION_HEARTBEAT_COOLDOWN = 3000

  trait Listener {
    def start(session: String) : Unit
    def stop(session:  String) : Unit
    def tick(session:  String) : Unit = {}
  }

  case class Options(name: String, ttl: Int = 45, interval: Int = 20, lockDelay: Int = 10)
}
