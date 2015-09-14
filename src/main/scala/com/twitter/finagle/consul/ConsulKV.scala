package com.twitter.finagle.consul

import com.twitter.finagle.httpx.Response
import org.json4s.jackson.JsonMethods._

object ConsulKV {

  implicit val format = org.json4s.DefaultFormats

  case class Key(Session: Option[String], CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: String)

  def decodeAcquire(response: Response): Boolean = {
    parse(response.contentString).extract[Boolean]
  }

  def decodeKey(response: Response): Key = {
    parse(response.contentString).extract[Key]
  }

  def decodeKeys(response: Response): List[Key] = {
    parse(response.contentString).extract[List[Key]]
  }
}
