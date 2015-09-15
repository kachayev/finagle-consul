package com.twitter.finagle.consul.client

import com.twitter.finagle.httpx.{Response, Request}
import com.twitter.finagle.{Service => HttpxService}

object Constants {
  type HttpClient   = HttpxService[Request, Response]
  type HttpRequest  = Request
  type HttpResponse = Response
}
