package com.cpc.spark.common

import com.cpc.spark.streaming.tools.NgLogParser
import com.cpc.spark.streaming.tools.Encoding

object Event {
  def parse_show_log(data: String): Show = {
    try {
      val log_map = NgLogParser.split_ng_log(data)
      val query = log_map("request")
      val ip = log_map("remote_addr")
      val referer = log_map("referer")
      val user_agent = log_map("user_agent")
      val body = query.split(" ")(1)
      if (body.startsWith("/show")) {
        val query_body = body.split("\\?")(1)
        val split_log_tmps = query_body.split("\\.")
        if (split_log_tmps.length == 2) {
          val head_byte = Encoding.base64Decoder(split_log_tmps(0))
          val head_event = eventprotocol.Protocol.Event.Head.parseFrom(head_byte.toArray)
          if (head_event.getCryptoType == eventprotocol.Protocol.Event.Head.CryptoType.JESGOO_BASE64) {
            val meds = Encoding.base64Decoder(split_log_tmps(1), head_event.getCryptoParam)
            new Show(meds, ip,referer ,user_agent)
          } else {
            null
          }
        } else {
          null
        }
      } else {
        null
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("show_error = " + data)
        null
    }
  }

  def parse_click_log(data: String): Click = {
    try {
      val tmps = data.split(" ")
      val d = Encoding.base64Decoder(tmps(tmps.length - 1))
      new Click(d)
    } catch {
      case e: Exception =>
        println("click_parse = " + data)
        null
    }
  }
}

class Show(data: Seq[Byte], ips:String, referer:String, user_agent:String) {
  val typed = 2
  val ip = ips
  val refer = referer
  val ua = user_agent
  val event = eventprotocol.Protocol.Event.Body.parseFrom(data.toArray)
}

class Click(data: Seq[Byte]) {
  val typed = 3
  val event = eventprotocol.Protocol.Event.parseFrom(data.toArray)
}