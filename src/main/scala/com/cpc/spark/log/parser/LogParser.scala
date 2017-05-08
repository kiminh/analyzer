package com.cpc.spark.log.parser

import java.net.InetAddress
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

  val traceRegex = """GET\s/trace\?iclicashsid=(\w+)&duration=(\d+)""".r

  def parseTraceLog(txt: String): UnionLog = {
    var log: UnionLog = null
    if (txt != null) {
      traceRegex.findFirstMatchIn(txt).foreach {
        m =>
          val sub = m.subgroups
          if (sub.length == 2) {
            log = UnionLog(
              searchid = sub(0),
              duration = sub(1).toInt
            )
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
}

