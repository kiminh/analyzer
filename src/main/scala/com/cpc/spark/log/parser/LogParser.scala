package com.cpc.spark.log.parser

import aslog.Aslog
import com.cpc.spark.streaming.tools.Encoding
import eventprotocol.Protocol
import com.cpc.spark.common.Event


/**
 * Created by Roy on 2017/4/18.
 */

case class UnionLog(
  searchid: String,
  timestamp: Int,

  mediaType: Int,
  mediaAppsid: String,

  adslotid: String,
  adslotType: Int,

  adnum: Int,
  isfill: Int,
  ideaid: Int,
  unitid: Int,
  planid: Int,

  country: Int,
  province: Int,
  city: Int,
  locisp: Int,

  uid: String,
  ua: String,
  os: Int,
  screenw: Int,
  screenh: Int,

  isshow: Int,
  showtime: Int,

  isclick: Int,
  clicktime: Int,
  antispamScore: Int,
  antispamRules: String)


case class ShowLog()

case class ClickLog()

object LogParser {

  val NoneUnionLog = UnionLog("", 0, 0, "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", 0, 0, 0, 0, 0, 0, 0, 0, "")

  def parseSearchLog(txt: String): UnionLog = {
    var log = NoneUnionLog
    val notice = Aslog.NoticeLogBody.parseFrom(decodeLog(txt).toArray)
    if (notice.getSearchid.length > 0) {
      log = log.copy(
        searchid = notice.getSearchid,
        timestamp = notice.getTimestamp,
        mediaType = notice.getMedia.getType.getNumber,
        mediaAppsid = notice.getMedia.getAppsid
      )
      if (notice.getAdslotCount > 0) {
        val slot = notice.getAdslot(0)
        log = log.copy(
          adslotid = slot.getId,
          adslotType = slot.getType.getNumber
        )
      }
      if (notice.getDspretCount > 0) {
        val dsp = notice.getDspret(0)
        log = log.copy(adnum = dsp.getAdnum)
      }
      if (notice.getAdsCount > 0) {
        val ad = notice.getAds(0)
        log = log.copy(
          isfill = 1,
          ideaid = ad.getAdid,
          unitid = ad.getGroupid,
          planid = ad.getPlanid
        )
      }
      val loc = notice.getLocation
      log = log.copy(
        country = loc.getCountry,
        province = loc.getProvince,
        city = loc.getCity,
        locisp = loc.getIsp
      )
      val device = notice.getDevice
      log = log.copy(
        uid = device.getUid,
        ua = device.getUseragent.toString,
        os = device.getOs.getNumber,
        screenw = device.getScreenW,
        screenh = device.getScreenH
      )
    }
    log
  }

  def parseShowLog(txt: String): UnionLog = {
    var log = NoneUnionLog
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      log = log.copy(
        isshow = 1,
        showtime = body.getEventTimestamp
      )
    }
    log
  }

  def parseClickLog(txt: String): UnionLog = {
    var log = NoneUnionLog
    val event = Protocol.Event.parseFrom(decodeLog(txt).toArray)
    if (event.getBody.getSearchId.length > 0) {
      val body = event.getBody
      log = log.copy(
        searchid = body.getSearchId,
        isclick = 1,
        clicktime = body.getEventTimestamp,
        antispamScore = body.getAntispam.getScore,
        antispamRules = body.getAntispam.getRulesList.toArray.toString
      )
    }
    log
  }

  def decodeLog(log: String): Seq[Byte] = {
    try{
      val tmps = log.split(" ")
      Encoding.base64Decoder(tmps(tmps.length - 1))
    }catch{
      case e:Exception =>
        e.printStackTrace()
        println("decode log body error = " + log)
        null
    }
  }
}
