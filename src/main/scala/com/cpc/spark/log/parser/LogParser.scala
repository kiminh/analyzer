package com.cpc.spark.log.parser

import java.net.{InetAddress, URI}
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.{Event, Ui}


/**
  * Created by Roy on 2017/4/18.
  */


object LogParser {

  def parseSearchLog(txt: String): UnionLog = {
    var log: UnionLog = null
    val data = Ui.parseData(txt)
    if (data != null) {
      val notice = data.ui
      val (date, hour) = getDateHourFromTime(notice.getTimestamp)
      log = UnionLog(
        searchid = notice.getSearchid,
        timestamp = notice.getTimestamp,
        network = notice.getNetwork.getType.getNumber,
        ip = notice.getNetwork.getIp,
        exptags = notice.getExptagsList.toArray.mkString(","),
        media_type = notice.getMedia.getType.getNumber,
        media_appsid = notice.getMedia.getAppsid,
        date = date,
        hour = hour
      )
      if (notice.getAdslotCount > 0) {
        val slot = notice.getAdslot(0)
        log = log.copy(
          adslotid = slot.getId,
          adslot_type = slot.getType.getNumber,
          floorbid = slot.getFloorbid,
          cpmbid = slot.getCpmbid
        )
      }
      if (notice.getDspReqInfoCount > 0) {
        val dsp = notice.getDspReqInfo(0)
        log = log.copy(adnum = dsp.getRetAdsNum)
      }
      if (notice.getAdsCount > 0) {
        val ad = notice.getAds(0)
        log = log.copy(
          isfill = 1,
          ideaid = ad.getAdid,
          unitid = ad.getGroupid,
          planid = ad.getPlanid,
          userid = ad.getUserid,
          adtype = ad.getType.getNumber,
          adsrc = ad.getSrc.getNumber,
          interaction = ad.getInteraction.getNumber,
          bid = ad.getBid,
          price = ad.getPrice,
          ctr = ad.getCtr,
          cpm = ad.getCpm
        )
      }
      val loc = notice.getLocation
      log = log.copy(
        country = loc.getCountry,
        province = loc.getProvince,
        city = loc.getCity,
        isp = loc.getIsp
      )
      val device = notice.getDevice
      log = log.copy(
        uid = device.getUid,
        ua = device.getUseragent.toStringUtf8,
        os = device.getOs.getNumber,
        screen_w = device.getScreenW,
        screen_h = device.getScreenH,
        brand = device.getBrand,
        model = device.getModel
      )
      val user = notice.getUserprofile
      val interests = user.getInterestsList.iterator()
      var interRows = Seq[String]()
      while (interests.hasNext) {
        val in = interests.next()
        if (in.getInterestid > 0) {
          interRows = interRows :+ "%d=%d".format(in.getInterestid, in.getScore)
        }
      }
      log = log.copy(
        sex = user.getSex,
        age = user.getAge,
        coin = user.getCoin,
        interests = interRows.mkString(",")
      )
    }
    log
  }

  def parseShowLog(txt: String): UnionLog = {
    var log: UnionLog = null
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      log = UnionLog(
        searchid = body.getSearchId,
        isshow = 1,
        show_timestamp = body.getEventTimestamp,
        show_ip = data.ip
      )
    }
    log
  }

  def parseClickLog(txt: String): UnionLog = {
    var log: UnionLog = null
    val data = Event.parse_click_log(txt)
    if (data != null) {
      val event = data.event
      if (event.getBody.getSearchId.length > 0) {
        val body = event.getBody
        val charge = body.getCharge
        log = UnionLog(
          searchid = body.getSearchId,
          isclick = 1,
          click_timestamp = body.getEventTimestamp,
          antispam_score = body.getAntispam.getScore,
          antispam_rules = body.getAntispam.getRulesList.toArray.mkString(","),
          click_ip = LongToIPv4(body.getEventIp.toLong)
        )
      }
    }
    log
  }

  //val txt = "222.47.165.147 - - [09/May/2017:00:01:13 +0800] \"GET /trace?t=stay&duration=10&iclicashsid=23df11e7c20f841dfda7f277d2049b5ad0c98d18&w=414&h=672&sw=414&sh=716&os=iOS&ref=http%3A%2F%2Fg.fastapi.net%2Fqa%3Fslotid%3D1021642%26adid%3D795774%26index%3D0%26pvid%3D1021642.148-07.1pncscz.69qo.2.opn5tf.405e%26rn%3DC%3A1021642.148-07.1pncscz.69qo.2.opn5tf.405e_oc153%26mobile%3D1%26r%3D8dd&v=1.0&p=140&_t=10 HTTP/1.1\" 200 43 \"http://cj.juseyx.com/b2/?iclicashsid=23df11e7c20f841dfda7f277d2049b5ad0c98d18\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 10_1_1 like Mac OS X) AppleWebKit/602.2.14 (KHTML, like Gecko) Mobile/14B100ua qukan_ios\" \"-\" \"-\" 0.000"

  val traceRegex = """GET\s/trace\?(.*)""".r

  def parseTraceLog(txt: String): TraceLog = {
    var log: TraceLog = null
    if (txt != null) {
      traceRegex.findFirstMatchIn(txt).foreach {
        m =>
          val sub = m.subgroups
          if (sub.length == 1) {
            val query = sub(0)
            log = TraceLog()
            query.split('&').foreach {
              x =>
                try {
                  val Array(k , vv) = x.trim.split("=", 2)
                  val v = vv.trim
                  k match {
                    case "t" => log = log.copy(trace_type = v)
                    case "iclicashsid" => log = log.copy(searchid = v)
                    case "o" => log = log.copy(device_orientation = toInt(v))
                    case "w" => log = log.copy(client_w = toFloat(v))
                    case "h" => log = log.copy(client_h = toFloat(v))
                    case "sw" => log = log.copy(screen_w = toFloat(v))
                    case "sh" => log = log.copy(screen_h = toFloat(v))
                    case "os" => log = log.copy(trace_os = v)
                    case "ref" => log = log.copy(trace_refer = v)
                    case "v" => log = log.copy(trace_version = v)
                    case "s" => log = log.copy(trace_click_count = toInt(v))
                    case "x" => log = log.copy(client_x = toFloat(v))
                    case "y" => log = log.copy(client_y = toFloat(v))
                    case "px" => log = log.copy(page_x = toFloat(v))
                    case "py" => log = log.copy(page_y = toFloat(v))
                    case "_t" => log = log.copy(trace_ttl = toInt(v))
                    case "p" => log = log.copy(scroll_top = toFloat(v))
                    case "op1" => log = log.copy(trace_op1 = v)
                    case "op2" => log = log.copy(trace_op2 = v)
                    case "op3" => log = log.copy(trace_op3 = v)
                    case "duration" => log = log.copy(duration = toInt(v))
                  }
                } catch {
                  case e: Exception => null
                }
            }
          }
      }
    }
    log
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val hourFormat = new SimpleDateFormat("HH")
  val partitionFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  /*
  t: seconds
   */
  def getDateHourFromTime(t: Int): (String, String) = {
    if (t > 0) {
      val dt = new Date(t.toLong * 1000L)
      val parts = partitionFormat.format(dt).split("/")
      if (parts.length == 2) {
        (parts(0), parts(1))
      } else {
        ("", "")
      }
    } else {
      ("", "")
    }
  }

  def IPv4ToLong(dottedIP: String): Long = {
    val addrArray: Array[String] = dottedIP.split("\\.")
    var num: Long = 0
    var i: Int = 0
    while (i < addrArray.length) {
      val power: Int = 3 - i
      num = num + ((addrArray(i).toInt % 256) * Math.pow(256, power)).toLong
      i += 1
    }
    num
  }

  def LongToIPv4 (ip : Long) : String = {
    val bytes: Array[Byte] = new Array[Byte](4)
    bytes(0) = ((ip & 0xff000000) >> 24).toByte
    bytes(1) = ((ip & 0x00ff0000) >> 16).toByte
    bytes(2) = ((ip & 0x0000ff00) >> 8).toByte
    bytes(3) = (ip & 0x000000ff).toByte
    InetAddress.getByAddress(bytes).getHostAddress()
  }

  def toInt(s: String): Int = {
    try {
      s.trim.toInt
    } catch {
      case e : Exception => 0
    }
  }

  def toFloat(s: String): Float = {
    try {
      s.trim.toFloat
    } catch {
      case e : Exception => 0
    }
  }
}

