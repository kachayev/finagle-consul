package com.twitter.finagle.consul

import com.twitter.finagle.httpx.Response
import org.json4s.jackson.JsonMethods._

object ConsulKV {

  implicit val format = org.json4s.DefaultFormats

  case class Key(Session: Option[String], CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: String)
  case class SessionGet(LockDelay: String, Checks: List[String], Node: String, ID: String, CreateIndex: Int)
  case class SessionCreate(ID: String)

  def decodeAcquire(response: Response): Boolean = {
    parse(response.contentString).extract[Boolean]
  }

  def decodeKey(response: Response): Key = {
    parse(response.contentString).extract[Key]
  }

  def decodeKeys(response: Response): List[Key] = {
    parse(response.contentString).extract[List[Key]]
  }

  def decodeSessionGet(response: Response): SessionGet = {
    parse(response.contentString).extract[SessionGet]
  }

  def decodeSessionCreate(response: Response): SessionCreate = {
    parse(response.contentString).extract[SessionCreate]
  }
}
