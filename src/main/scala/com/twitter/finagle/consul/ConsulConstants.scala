package com.twitter.finagle.consul

trait ConsulConstants {
  val SESSION_HEARTBEAT_COOLDOWN = 5000

  val SESSION_CREATE_PATH  = "/v1/session/create"
  val SESSION_DESTROY_PATH = "/v1/session/destroy/%s"
  val SESSION_RENEW_PATH   = "/v1/session/renew/%s"
  val SESSION_INFO_PATH    = "/v1/session/info/%s"
}
