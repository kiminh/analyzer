package com.cpc.spark.log.parser

import java.net.{InetAddress, URLDecoder}
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.{Event, Ui, Cfg}
import org.apache.spark.sql.types._

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/18.
  */


object LogParser {

  def parseSearchLog(txt: String): UnionLog = {
    var log: UnionLog = null
    val srcData = Ui.parseData(txt)
    if (srcData != null) {
      val notice = srcData.ui
      val (date, hour) = getDateHourFromTime(notice.getTimestamp)
      val ext = mutable.Map[String, ExtValue]()
      log = UnionLog(
        searchid = notice.getSearchid,
        timestamp = notice.getTimestamp,
        network = notice.getNetwork.getType.getNumber,
        ip = notice.getNetwork.getIp,
        exptags = notice.getExptagsList.toArray.filter(_ != "").mkString(","),
        media_type = notice.getMedia.getType.getNumber,
        media_appsid = notice.getMedia.getAppsid,
        date = date,
        hour = hour
      )
      ext.update("client_type", ExtValue(string_value = notice.getClient.getType.name()))
      ext.update("client_version", ExtValue(string_value = "%s.%s.%s.%s".format(notice.getClient.getVersion.getMajor,
        notice.getClient.getVersion.getMinor,notice.getClient.getVersion.getMicro,notice.getClient.getVersion.getBuild)))
      ext.update("media_site_url", ExtValue(string_value = notice.getMedia.getSite.getUrls))
      var devices = ""
      val deviceCount = notice.getDevice.getIdsCount-1
      for  (i <- 0 to  deviceCount){
        if(devices.length >0){
          devices += ";" + notice.getDevice.getIds(i).getType+":"+ notice.getDevice.getIds(i).getId
        }else{
          devices +=  notice.getDevice.getIds(i).getType + ":" + notice.getDevice.getIds(i).getId
        }
      }
      ext.update("device_ids", ExtValue(string_value = devices))
      if (notice.getAdslotCount > 0) {
        val slot = notice.getAdslot(0)
        log = log.copy(
          adslotid = slot.getId,
          adslot_type = slot.getType.getNumber,
          floorbid = slot.getFloorbid,
          cpmbid = slot.getCpmbid
        )
        ext.update("channel", ExtValue(string_value = slot.getChannel))
        ext.update("pagenum", ExtValue(int_value = slot.getPagenum))
        ext.update("bookid", ExtValue(string_value = slot.getBookid))
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
        ext.update("cvr_threshold", ExtValue(int_value = ad.getCvrThres.toInt))
        ext.update("exp_ctr", ExtValue(int_value = ad.getTitlectr.toInt))
        ext.update("exp_cvr", ExtValue(int_value = ad.getCvr.toInt))
        ext.update("adclass", ExtValue(int_value = ad.getClass_))
        ext.update("usertype", ExtValue(int_value = ad.getUsertype))
        ext.update("trigger_type", ExtValue(int_value = ad.getTriggerType))
        ext.update("rank_discount", ExtValue(int_value = ad.getDiscount))
        ext.update("user_req_ad_num", ExtValue(int_value = ad.getShowCount))

        val mcount = ad.getMaterialidCount
        if (mcount > 0) {
          val ids = new Array[Int](mcount)
          for (i <- 0 until mcount) {
            ids(i) = ad.getMaterialid(i)
          }
          ext.update("materialid", ExtValue(string_value = ids.mkString(" ")))
        }
      }

      val loc = notice.getLocation
      log = log.copy(
        country = loc.getCountry,
        province = loc.getProvince,
        city = loc.getCity,
        isp = loc.getIsp
      )
      ext.update("city_level", ExtValue(int_value = loc.getCityLevel))

      val media_app = notice.getMedia.getApp
      ext.update("media_app_packagename", ExtValue(string_value = media_app.getPackagename))
      ext.update("media_app_version", ExtValue(string_value = media_app.getVersion))

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
      ext.update("phone_price", ExtValue(int_value = device.getPhoneprice))
      ext.update("phone_level", ExtValue(int_value = device.getPhonelevel))
      ext.update("brand_title", ExtValue(string_value = device.getBrandTitle))
      val osv = device.getOsversion
      ext.update("os_version", ExtValue(string_value = "%d.%d.%d.%d"
        .format(osv.getMajor, osv.getMinor, osv.getMicro, osv.getBuild)))
      val user = notice.getUserprofile
      val interests = user.getInterestsList.iterator()
      var interRows = Seq[String]()
      while (interests.hasNext) {
        val in = interests.next()
        if (in.getInterestid > 0) {
          interRows = interRows :+ "%d=%d".format(in.getInterestid, in.getScore)
        }
      }
      ext.update("antispam", ExtValue(int_value = user.getAntispam))
      ext.update("share_coin", ExtValue(int_value = user.getShareCoin))
      ext.update("qukan_new_user", ExtValue(int_value = user.getNewuser))
      ext.update("user_req_num", ExtValue(int_value = user.getReqCount))
      log = log.copy(
        sex = user.getSex,
        age = user.getAge,
        coin = user.getCoin,
        interests = interRows.mkString(","),
        ext = ext.toMap
      )
    }
    log
  }

  def parseShowLog(txt: String): UnionLog = {
    var log: UnionLog = null
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      val ext = mutable.Map[String, ExtValue]()
      ext.update("show_refer", ExtValue(string_value = data.refer))
      ext.update("show_ua", ExtValue(string_value = data.ua))
      log = UnionLog(
        searchid = body.getSearchId,
        isshow = 1,
        show_timestamp = body.getEventTimestamp,
        show_ip = data.ip ,
        ext = ext
      )
    }
    log
  }
  def parseCfgLog(txt: String): CfgLog = {
    var log: CfgLog = null
    val data = Cfg.parseData(txt)
    if (data != null) {
      val body = data.cfg
      var aid = body.getAdSlotId
      val (date, hour) = getDateHourFromTime(body.getTimestamp)
      if(aid.length <= 0){
        aid = body.getAidList.toArray().mkString(",")
      }
      log = CfgLog(
        aid = aid,
        log_type = body.getUrlPath,
        search_timestamp = body.getTimestamp,
        request_url = body.getRequestUrl,
        resp_body = body.getRespBody,
        redirect_url = body.getRedirectUrl,
        template_conf = body.getTemplateConfList().toArray.mkString(","),
        adslot_conf = body.getAdslotConf,
        date = date,
        hour = hour
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
        val extra = event.getExtra
        val ext = mutable.Map[String, ExtValue]()
        ext.update("touch_x", ExtValue(int_value = extra.getTouchX))
        ext.update("touch_y", ExtValue(int_value = extra.getTouchY))
        ext.update("slot_width", ExtValue(int_value = extra.getWidth))
        ext.update("slot_height", ExtValue(int_value = extra.getHeight))
        ext.update("antispam_predict", ExtValue(float_value = body.getAntispam.getPredict))
        ext.update("click_ua", ExtValue(string_value = body.getAction.getUserAgent))
        log = UnionLog(
          searchid = body.getSearchId,
          isclick = 1,
          click_timestamp = body.getEventTimestamp,
          antispam_score = body.getAntispam.getScore,
          antispam_rules = body.getAntispam.getRulesList.toArray.mkString(","),
          click_ip = LongToIPv4(body.getEventIp.toLong),
          ext = ext.toMap
        )
      }
    }
    log
  }

  def parseClickLog2(txt: String): UnionLog = {
    var log: UnionLog = null
    val data = Event.parse_click_log(txt)
    if (data != null) {
      val event = data.event
      val (date, hour) = getDateHourFromTime(event.getBody.getEventTimestamp)
      if (event.getBody.getSearchId.length > 0) {
        val body = event.getBody
        val extra = event.getExtra
        val ext = mutable.Map[String, ExtValue]()
        ext.update("touch_x", ExtValue(int_value = extra.getTouchX))
        ext.update("touch_y", ExtValue(int_value = extra.getTouchY))
        ext.update("antispam_predict", ExtValue(float_value = body.getAntispam.getPredict))
        ext.update("phone_price", ExtValue(int_value = body.getDevice.getPhoneprice))
        ext.update("phone_level", ExtValue(int_value = body.getDevice.getPhonelevel))
        ext.update("click_referer", ExtValue(string_value = body.getEventReferer))
        ext.update("event_isp_tag", ExtValue(int_value = body.getEventIspTag))
        ext.update("click_ua", ExtValue(string_value = body.getAction.getUserAgent))
        //ext.update("adclass", ExtValue(int_value = body.getAd.getClass_))
        val interests = body.getUserprofile.getInterestsList.iterator()
        var interRows = Seq[String]()
        while (interests.hasNext) {
          val in = interests.next()
          if (in.getInterestid > 0) {
            interRows = interRows :+ "%d=%d".format(in.getInterestid, in.getScore)
          }
        }
        log = UnionLog(
          searchid = body.getSearchId,
          timestamp = body.getSearchTimestamp,
          network = body.getNetwork.getType.getNumber,
          ip = body.getNetwork.getIp,
          exptags = body.getExptagsList.toArray.filter(_ != "").mkString(","),
          media_appsid = body.getMedia.getMediaId,
          date = date,
          hour = hour,
          adslotid = body.getMedia.getAdslotId,
          adslot_type = body.getMedia.getAdslotType.getNumber,
          isclick = 1,
          click_timestamp = body.getEventTimestamp,
          antispam_score = body.getAntispam.getScore,
          antispam_rules = body.getAntispam.getRulesList.toArray.mkString(","),
          click_ip = LongToIPv4(body.getEventIp.toLong) ,
          isfill = 1,
          ideaid = body.getAd.getUnitId,
          unitid = body.getAd.getGroupId,
          planid = body.getAd.getPlanId,
          userid = body.getAd.getUserId,
          adtype = body.getAd.getType.getNumber,
          interaction = body.getAd.getInteraction.getNumber,
          price = body.getCharge.getPrice,
          country = body.getRegion.getCountry,
          province = body.getRegion.getProvince,
          city = body.getRegion.getCity,
          isp = body.getRegion.getIsp,
          uid = body.getDevice.getUid,
          ua = body.getDevice.getUseragent.toStringUtf8,
          os = body.getDevice.getOs.getNumber,
          screen_w = body.getDevice.getScreenW,
          screen_h = body.getDevice.getScreenH,
          brand = body.getDevice.getBrand,
          model = body.getDevice.getModel,
          sex = body.getUserprofile.getSex,
          age = body.getUserprofile.getAge,
          coin = body.getUserprofile.getCoin,
          interests = interRows.mkString(","),
          ext = ext.toMap
        )
      }
    }
    log
  }
  //val txt = "36.149.39.90 - - [15/May/2017:08:02:36 +0800] \"GET /trace?t=stay&duration=1&iclicashsid=90c60d5e0fc887090984f5589aaa157a62207613&w=980&h=1306&sw=360&sh=640&os=Android&ref=http%3A%2F%2Fcj.juseyx.com%2Fb2%2F%3Ficlicashsid%3D90c60d5e0fc887090984f5589aaa157a62207613&v=1.0&_t=0 HTTP/1.1\" 200 43 \"http://cj.juseyx.com/b2/?iclicashsid=90c60d5e0fc887090984f5589aaa157a62207613&t=1494806555248\" \"Mozilla/5.0 (Linux; Android 6.0; NEM-TL00 Build/HONORNEM-TL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36 qukan_android\" \"-\" \"-\" 0.000"

  val traceRegex = """GET\s/trace\?([^\s]+)""".r

  def parseTraceLog(txt: String): TraceLog = {
    var log: TraceLog = null
    if (txt != null) {
      traceRegex.findFirstMatchIn(txt).foreach {
        m =>
          val sub = m.subgroups
          if (sub.length == 1) {
            log = TraceLog()
            sub(0).split('&').foreach {
              x =>
                try {
                  val Array(k , vv) = x.trim.split("=", 2)
                  val v = URLDecoder.decode(vv.trim, "UTF8")
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
                    case "auto" => log = log.copy(auto = toAutoInt(v))
                    case _ =>
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
  def parseHuDongTraceLog(txt: String): HuDongLog = {
    var log: HuDongLog = null
    if (txt != null) {
      traceRegex.findFirstMatchIn(txt).foreach {
        m =>
          val sub = m.subgroups
          if (sub.length == 1) {
            log = HuDongLog()
            sub(0).split('&').foreach {
              x =>
                try {
                  val Array(k , vv) = x.trim.split("=", 2)
                  val v = URLDecoder.decode(vv.trim, "UTF8")
                  k match {
                    case "t" => log = log.copy(log_type = v)
                    case "adslot_id" => log = log.copy(adslot_id = v)
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
  /*
  t: seconds
   */
  def getDateHourFromTime(t: Int): (String, String) = {
    if (t > 0) {
      val dt = new Date(t.toLong * 1000L)
      val parts = new SimpleDateFormat("yyyy-MM-dd/HH").format(dt).split("/")
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
    InetAddress.getByAddress(bytes).getHostAddress
  }

  def toInt(s: String): Int = {
    try {
      s.trim.toInt
    } catch {
      case e : Exception => 0
    }
  }
  def toAutoInt(s: String): Int = {
    try {
      if(s.indexOf("1") >= 0){
        return 1
      }else{
        return s.trim.toInt
      }
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


