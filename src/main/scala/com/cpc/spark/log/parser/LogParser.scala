package com.cpc.spark.log.parser

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.{Event, Ui}


/**
  * Created by Roy on 2017/4/18.
  */

case class UnionLog(
                     searchid: String = "",
                     timestamp: Int = 0,
                     network: Int = 0,
                     ip: String = "",
                     exptags: String = "",
                     media_type: Int = 0,
                     media_appsid: String = "",
                     adslotid: String = "",
                     adslot_type: Int = 0,
                     adnum: Int = 0,
                     isfill: Int = 0,
                     adtype: Int = 0,
                     adsrc: Int = 0,
                     interaction: Int = 0,
                     bid: Int = 0,
                     floorbid: Float = 0,
                     cpmbid: Float = 0,
                     price: Int = 0,
                     ctr: Long = 0,
                     cpm: Long = 0,
                     ideaid: Int = 0,
                     unitid: Int = 0,
                     planid: Int = 0,
                     country: Int = 0,
                     province: Int = 0,
                     city: Int = 0,
                     isp: Int = 0,
                     brand: String = "",
                     model: String = "",
                     uid: String = "",
                     ua: String = "",
                     os: Int = 0,
                     screen_w: Int = 0,
                     screen_h: Int = 0,
                     sex: Int = 0,
                     age: Int = 0,
                     coin: Int = 0,
                     isshow: Int = 0,
                     show_timestamp: Int = 0,
                     show_network: Int = 0,
                     show_ip: String = "",
                     isclick: Int = 0,
                     click_timestamp: Int = 0,
                     click_network: Int = 0,
                     click_ip: String = "",
                     antispam_score: Int = 0,
                     antispam_rules: String = "",
                     duration: Int = 0,
                     date: String = "",
                     hour: String = ""
                   )

object LogParser {

  def parseSearchLog(txt: String): UnionLog = {
    var log = UnionLog()
    val data = Ui.parseData(txt)
    if (data != null) {
      val notice = data.ui
      log = log.copy(
        searchid = notice.getSearchid,
        timestamp = notice.getTimestamp,
        network = notice.getNetwork.getType.getNumber,
        ip = notice.getNetwork.getIp,
        exptags = notice.getExptagsList.toArray.mkString(","),
        media_type = notice.getMedia.getType.getNumber,
        media_appsid = notice.getMedia.getAppsid
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
          planid = ad.getPlanid,
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
      log = log.copy(
        sex = user.getSex,
        age = user.getAge,
        coin = user.getCoin
      )
    }
    val (date, hour) = getDateHourFromTime(log.timestamp)
    log.copy(date = date, hour = hour)
  }

  def parseShowLog(txt: String): UnionLog = {
    var log = UnionLog()
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      log = log.copy(
        searchid = body.getSearchId,
        isshow = 1,
        show_timestamp = body.getEventTimestamp
      )
    }
    val (date, hour) = getDateHourFromTime(log.timestamp)
    log.copy(date = date, hour = hour)
  }

  def parseClickLog(txt: String): UnionLog = {
    var log = UnionLog()
    val data = Event.parse_click_log(txt)
    if (data != null) {
      val event = data.event
      if (event.getBody.getSearchId.length > 0) {
        val body = event.getBody
        log = log.copy(
          searchid = body.getSearchId,
          isclick = 1,
          click_timestamp = body.getEventTimestamp,
          antispam_score = body.getAntispam.getScore,
          antispam_rules = body.getAntispam.getRulesList.toArray.mkString(",")
        )
      }
    }
    val (date, hour) = getDateHourFromTime(log.timestamp)
    log.copy(date = date, hour = hour)
  }

  val traceRegex = """GET\s/trace\?iclicashsid=(\w+)&duration=(\d+)""".r

  def parseTraceLog(txt: String): UnionLog = {
    var log = UnionLog()
    val m = traceRegex.findFirstMatchIn(txt).foreach {
      m =>
        val sub = m.subgroups
        if (sub.length == 2) {
          log = log.copy(
            searchid = sub(0),
            duration = sub(1).toInt
          )
        }
    }
    log
  }

  val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  val hourFormat = new SimpleDateFormat("HH")

  /*
  t: seconds
   */
  def getDateHourFromTime(t: Int): (String, String) = {
    if (t > 0) {
      val dt = new Date(t.toLong * 1000L)
      (dateFormat.format(dt), hourFormat.format(dt))
    } else {
      ("", "")
    }
  }
}

