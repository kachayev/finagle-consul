package com.twitter.finagle.consul

import com.twitter.finagle.httpx.Response

object ConsulErrors {
  class BadResponseException(msg: String) extends RuntimeException(msg)

  private[consul] def badResponse(reply: Response) = {
    new BadResponseException(s"code=${reply.getStatusCode()} body=${reply.contentString}")
  }
}
