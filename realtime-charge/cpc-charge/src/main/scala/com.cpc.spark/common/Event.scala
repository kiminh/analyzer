package com.cpc.spark.common

import com.cpc.spark.streaming.tools.NgLogParser
import com.cpc.spark.streaming.tools.Encoding
import eventprotocol.Protocol

object Event {
  def parse_show_log(data1: String): Show = {
    val data = data1.replaceAll("%20", "")
    try {
      val log_map = NgLogParser.split_ng_log(data)
      val query = log_map("request")
      val body = query.split(" ")(1)
      if (body.startsWith("/show")) {
        val query_body = body.split("\\?")(1)
        val split_log_tmps = query_body.split("\\.")
        if (split_log_tmps.length == 2) {
          val head_byte = Encoding.base64Decoder(split_log_tmps(0))
          val head_event = Protocol.Event.Head.parseFrom(head_byte.toArray)
          //小米设备存在&value=参数，去掉
          val sourceProtobufBody = split_log_tmps(1).split("&")
          val protobufBody = sourceProtobufBody(0)
          if (head_event.getCryptoType == Protocol.Event.Head.CryptoType.JESGOO_BASE64) {
            val meds = Encoding.base64Decoder(protobufBody, head_event.getCryptoParam)
            new Show(meds)
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

class Show(data: Seq[Byte]) {
  val typed = 2
  val event = Protocol.Event.Body.parseFrom(data.toArray)
}

class Click(data: Seq[Byte]) {
  val typed = 3
  val event = Protocol.Event.parseFrom(data.toArray)
}