package com.twitter.finagle.consul.client

import java.util.Base64

import com.twitter.finagle.consul.ConsulErrors
import com.twitter.finagle.httpx.{Method, Request => HttpRequest, Response => HttpResponse}
import com.twitter.finagle.{Service => HttpxService}
import com.twitter.util.Future
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

class KeyService(httpClient: HttpxService[HttpRequest, HttpResponse]) {

  import KeyService._

  implicit val format = org.json4s.DefaultFormats

  def get(path: String): Future[Option[Response[String]]] = {
    val httpRequest = HttpRequest(Method.Get, keyName(path))
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { httpResponse =>
      httpResponse.getStatusCode() match {
        case 200 =>
          val reply: Option[Response[String]] = Option(decodeHttpResponseSetAsString(httpResponse).head)
          Future.value(reply)
        case 404 =>
          Future.value(None)
        case _   =>
          Future.exception(ConsulErrors.badResponse(httpResponse))
      }
    }
  }

  def getJson[T](path: String)(implicit m: Manifest[T]): Future[Option[Response[T]]] = {
    val httpRequest = HttpRequest(Method.Get, keyName(path))
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { httpResponse =>
      httpResponse.getStatusCode() match {
        case 200 =>
          val reply: Option[Response[T]] = Option(decodeHttpResponseSetAsJson(httpResponse).head)
          Future.value(reply)
        case 404 =>
          Future.value(None)
        case _   =>
          Future.exception(ConsulErrors.badResponse(httpResponse))
      }
    }
  }

  def getJsonSet[T](path: String)(implicit m: Manifest[T]): Future[Set[Response[T]]] = {
    val httpRequest = HttpRequest(Method.Get, keyName(path, recurse = true))
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { httpResponse =>
      httpResponse.getStatusCode() match {
        case 200 =>
          val reply: Set[Response[T]] = decodeHttpResponseSetAsJson(httpResponse)
          Future.value(reply)
        case 404 =>
          Future.value(Set.empty)
        case _   =>
          Future.exception(ConsulErrors.badResponse(httpResponse))
      }
    }
  }

  def putJson[A <: AnyRef](path: String, body: A)(implicit ma: Manifest[A]): Future[Option[Response[A]]] = {
    val httpRequest = HttpRequest(Method.Put, keyName(path))
    httpRequest.setContentTypeJson()
    val httpBody: String = Serialization.write[A](body)
    httpRequest.write(httpBody)
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => getJson(path)
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def acquireJson[A <: AnyRef](path: String, body: A, session: String): Future[Boolean] = {
    val httpRequest = HttpRequest(Method.Put, keyName(path, acquire = Some(session)))
    httpRequest.setContentTypeJson()
    val httpBody: String = Serialization.write[A](body)
    httpRequest.write(httpBody)
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(decodeHttpResponseAsSingleBoolean(reply))
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def releaseJson[A <: AnyRef](path: String, body: A, session: String): Future[Boolean] = {
    val httpRequest = HttpRequest(Method.Put, keyName(path, release = Some(session)))
    httpRequest.setContentTypeJson()
    val httpBody: String = Serialization.write[A](body)
    httpRequest.write(httpBody)
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(decodeHttpResponseAsSingleBoolean(reply))
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  def delete(path: String): Future[Unit] = {
    val httpRequest = HttpRequest(Method.Delete, keyName(path))
    httpRequest.setContentTypeJson()
    httpClient(httpRequest) flatMap { reply =>
      reply.getStatusCode() match {
        case 200 => Future.value(Unit)
        case _   => Future.exception(ConsulErrors.badResponse(reply))
      }
    }
  }

  private def decodeHttpResponseSetAsJson[T](httpResponse: HttpResponse)(implicit m: Manifest[T]): Set[Response[T]] = {
    val encodedReplies = parse(httpResponse.contentString).extract[Set[Response[String]]]
    encodedReplies map { encodedReply  =>
      val decodedValue = decodeJsonValue[T](encodedReply.Value)
      val reply: Response[T] = encodedReply.copy[T](Value = decodedValue)
      reply
    }
  }

  private def decodeHttpResponseSetAsString(httpResponse: HttpResponse): Set[Response[String]] = {
    parse(httpResponse.contentString).extract[Set[Response[String]]]
  }

  private def decodeHttpResponseAsSingleBoolean(httpResponse: HttpResponse): Boolean = {
    parse(httpResponse.contentString).extract[Boolean]
  }

  private def decodeJsonValue[T](bytes: String)(implicit m: Manifest[T]): T = {
    parse(decodeStringValue(bytes)).extract[T]
  }

  private def decodeStringValue(bytes: String): String = {
    new String(Base64.getDecoder.decode(bytes))
  }

  private def keyName(path: String, recurse: Boolean = false, acquire: Option[String] = None, release: Option[String] = None): String = {
    val key = s"/v1/kv/$path"
    var opts = List.empty[String]

    if (recurse) {
      opts = opts ++ List("recurse")
    }

    acquire match {
      case Some(id) =>
        opts = opts ++ List(s"acquire=$id")
      case None =>
    }

    release match {
      case Some(id) =>
        opts = opts ++ List(s"release=$id")
      case None =>
    }

    if(opts.isEmpty) {
      key
    } else {
      s"$key?${opts.mkString("&")}"
    }
  }
}

object KeyService {
  case class Response[T](Session: Option[String], CreateIndex: Int, ModifyIndex: Int, LockIndex: Int, Key: String, Flags: Int, Value: T)

  def apply(httpClient: HttpxService[HttpRequest, HttpResponse]) = new KeyService(httpClient)
}