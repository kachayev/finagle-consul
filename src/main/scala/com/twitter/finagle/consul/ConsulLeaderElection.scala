package com.twitter.finagle.consul

import java.util.logging.{Level, Logger}

import com.twitter.finagle.Service
import com.twitter.finagle.consul.client.KeyService
import com.twitter.finagle.httpx.{Request, Response}
import com.twitter.util._

class ConsulLeaderElection(name: String, httpClient: Service[Request,Response], session: ConsulSession) {

  import ConsulLeaderElection.Status._

  private val lockName = s"finagle/leader/$name"
  private var status = Pending
  private val self   = this
  private val client = KeyService(httpClient)
  private val log    = Logger.getLogger(getClass.getName)

  private val listener = new Object with ConsulSession.Listener {
    def start(session: String): Unit =         self.checkOrAcquireLock(session)
    def stop(session: String): Unit =          self.releaseLock(session)
    override def tick(session: String): Unit = self.checkOrAcquireLock(session)
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

  private def checkOrAcquireLock(session: String): Unit = {
    val newStatus =
      status match {
        case Leader  => checkLock(session)
        case Pending => acquireLock(session)
      }
    if (newStatus != status) {
      logNewStatus(session, newStatus)
      status = newStatus
    }
  }

  private def logNewStatus(session: String, newStatus: Value): Unit = {
    if (newStatus == Leader) {
      log.info(s"Consul become a leader name=$name session=$session")
    }
    if (newStatus == Pending) {
      log.info(s"Consul become a slave name=$name session=$session")
    }
  }

  private def checkLock(session: String): Value = {
    val reply = Await.result(client.get(lockName).liftToTry)
    reply match {
      case Return(Some(value)) if value.Session.contains(session) =>
        Leader
      case Return(_) =>
        Pending
      case Throw(e) =>
        log.log(Level.SEVERE, e.getMessage, e)
        Pending
    }
  }

  private def acquireLock(session: String): Value = {
    val reply = Await.result(client.acquire(lockName, session).liftToTry)
    reply match {
      case Return(true) =>
        Leader
      case Return(_) =>
        Pending
      case Throw(e) =>
        log.log(Level.SEVERE, e.getMessage, e)
        Pending
    }
  }

  private def releaseLock(session: String): Unit = {
    if (status == Leader) {
      val reply = Await.result(client.release(lockName, session).liftToTry)
      reply match {
        case Return(true) =>
          log.info(s"Consul lock released name=$name session=$session")
        case Return(_) =>
          log.severe(s"Consul lock release fail, locked by another session name=$name session=$session")
        case Throw(e) =>
          log.log(Level.SEVERE, e.getMessage, e)
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
