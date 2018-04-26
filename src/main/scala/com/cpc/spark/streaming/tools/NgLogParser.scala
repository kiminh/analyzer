package com.cpc.spark.streaming.tools
import java.text.SimpleDateFormat
import java.util.Locale
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

object NgLogParser {

  def parse_ng_time(time: String): Long = {
    val minuteFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    val t = minuteFormat.parse(time.split(" ")(0).trim()).getTime.toLong
    Utils.formate_time(t)
  }

  val STANDARD_NGINX_LOG_FIELDS = Array(
    "remote_addr",
    "None",
    "remote_user",
    "time_local",
    "request",
    "status",
    "body_bytes_sent",
    "referer",
    "user_agent",
    "x_forwarded_for",
    "request_body")

  def split_ng_log(log: String): HashMap[String, String] = {
    val list = new ListBuffer[String]
    var start = 0
    val log_length = log.length
    while (start < log_length) {
      if (log(start) == '"') {
        start += 1
        var end = log.indexOf("\"", start)
        if (end == -1) {
          end = log_length
        }
        list.append(log.substring(start, end))
        start = end + 2
      } else if (log(start) == '[') {
        start += 1
        var end = log.indexOf("]", start)
        if (end == -1) {
          end = log_length
        }
        list.append(log.substring(start, end))
        start = end + 2
      } else {
        var end = log.indexOf(" ", start)
        if (end == -1) {
          end = log_length
        }
        list.append(log.substring(start, end))
        start = end + 1
      }
    }
    val map = new HashMap[String, String]
    for (i <- 0 to STANDARD_NGINX_LOG_FIELDS.length - 1) {
      if (i < list.length) {
        if (!STANDARD_NGINX_LOG_FIELDS(i).equalsIgnoreCase("None")) {
          map += (STANDARD_NGINX_LOG_FIELDS(i) -> list(i))
        }
      }
    }
    return map
  }
}