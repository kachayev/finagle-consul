package com.twitter.finagle.consul

import java.util.logging.Logger

import com.twitter.finagle.Service
import com.twitter.finagle.httpx.{Method, Request, Response}
import com.twitter.util._

class ConsulLeaderElection(name: String, client: Service[Request,Response], session: ConsulSession)
  extends ConsulConstants {

  import ConsulLeaderElection.Status._

  private var status = Pending
  private val self   = this
  private val log    = Logger.getLogger(getClass.getName)

  private val listener = new Object with ConsulSession.Listener {
    def start(id: ConsulSession.SessionId): Unit =         self.checkOrAcquireLock(id)
    def stop(id: ConsulSession.SessionId): Unit =          self.releaseLock(id)
    override def tick(id: ConsulSession.SessionId): Unit = self.checkOrAcquireLock(id)
  }

  def start(): Unit = {
    session.addListener(listener)
    session.start()
  }

  def stop(): Unit = {
    session.stop()
    session.delListener(listener)
  }

  def getStatus = status

  private def checkOrAcquireLock(sessionId: ConsulSession.SessionId): Unit = {
    val newStatus =
      status match {
        case Leader  => checkLock(sessionId)
        case Pending => acquireLock(sessionId)
      }
    if (newStatus != status) {
      logNewStatus(sessionId, newStatus)
      status = newStatus
    }
  }

  private def logNewStatus(sessionId: ConsulSession.SessionId, newStatus: ConsulLeaderElection.Status.Value): Unit = {
    if (newStatus == Leader) {
      log.info(s"Consul become a leader name=$name session=$sessionId")
    }
    if (newStatus == Pending) {
      log.info(s"Consul become a leader name=$name session=$sessionId")
    }
  }

  private def checkLock(sessionId: ConsulSession.SessionId): ConsulLeaderElection.Status.Value = {
    val req    = Request(Method.Get, LEADER_CHECK_PATH.format(name))
    Await.result(client(req).liftToTry) match {
      case Return(reply) if reply.getStatusCode() == 200 =>
        val res = ConsulKV.decodeKey(reply)
        res.Session match {
          case Some(sid) if sid == sessionId => Leader
          case _ => Pending
        }
      case Return(reply) =>
        val e = ConsulErrors.badResponse(reply)
        log.severe(s"Consul leader lock acquire error ${e.getClass} - ${e.getMessage}")
        Pending
      case Throw(e) =>
        log.severe(s"Consul leader lock acquire error ${e.getClass} - ${e.getMessage}")
        Pending
    }
  }

  private def acquireLock(sessionId: ConsulSession.SessionId): ConsulLeaderElection.Status.Value = {
    val req    = Request(Method.Put, LEADER_ACQUIRE_PATH.format(name, sessionId))
    val result = Await.result(client(req).liftToTry)
    result match {
      case Return(reply) if reply.getStatusCode() == 200 =>
        if (ConsulKV.decodeAcquire(reply)) {
          Leader
        } else {
          Pending
        }
      case Return(reply) =>
        val e = ConsulErrors.badResponse(reply)
        log.severe(s"Consul leader lock acquire error ${e.getClass} - ${e.getMessage}")
        Pending
      case Throw(e) =>
        log.severe(s"Consul leader lock acquire error ${e.getClass} - ${e.getMessage}")
        Pending
    }
  }

  private def releaseLock(sessionId: ConsulSession.SessionId): Unit = {
    if (status == Leader) {
      val req    = Request(Method.Put, LEADER_RELEASE_PATH.format(name, sessionId))
      val result = Await.result(client(req).liftToTry)
      result match {
        case Return(reply) if reply.getStatusCode() == 200 =>
          log.info(s"Consul lock released name=$name session=$sessionId")
        case Return(reply) =>
          val e = ConsulErrors.badResponse(reply)
          log.severe(s"Consul leader lock release error ${e.getClass} - ${e.getMessage}")
        case Throw(e) =>
          log.severe(s"Consul leader lock release error ${e.getClass} - ${e.getMessage}")
      }
    }
  }
}

object ConsulLeaderElection {

  object Status extends Enumeration {
    val Pending, Leader  = Value
  }

  def get(name: String, hosts: String): ConsulLeaderElection = {
    val client  = ConsulClientFactory.getClient(hosts)
    val session = ConsulSessionFactory.getSession(hosts)
    val leader  = new ConsulLeaderElection(name, client, session)
    leader.start()
    leader
  }

}
