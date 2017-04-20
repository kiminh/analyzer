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
  network: Int,
  ip: String,
  exptags: String,

  media_type: Int,
  media_appsid: String,

  adslotid: String,
  adslot_type: Int,

  adnum: Int,
  isfill: Int,
  adtype: Int,
  adsrc: Int,
  interaction: Int,
  bid: Int,
  floorbid: Float,
  cpmbid: Float,
  price: Int,
  ctr: Long,
  cpm: Long,
  ideaid: Int,
  unitid: Int,
  planid: Int,

  country: Int,
  province: Int,
  city: Int,
  isp: Int,

  uid: String,
  ua: String,
  os: Int,
  screen_w: Int,
  screen_h: Int,
  brand: String,
  model: String,

  sex: Int,
  age: Int,
  coin: Int,

  isshow: Int,
  show_timestamp: Int,
  show_network: Int,
  show_ip: String,


  isclick: Int,
  click_timestamp: Int,
  click_network: Int,
  click_ip: String,
  antispam_score: Int,
  antispam_rules: String,
  date: String,
  hour: String)


case class ShowLog()

case class ClickLog()

object LogParser {

  val EmptyUnionLog = UnionLog(
    searchid = "",
    timestamp = 0,
    network = 0,
    ip = "",
    exptags = "",
    media_type = 0,
    media_appsid = "",
    adslotid = "",
    adslot_type = 0,
    adnum = 0,
    isfill = 0,
    adtype = 0,
    adsrc = 0,
    interaction = 0,
    bid = 0,
    floorbid = 0,
    cpmbid = 0,
    price = 0,
    ctr = 0,
    cpm = 0,
    ideaid = 0,
    unitid = 0,
    planid = 0,
    country = 0,
    province = 0,
    city = 0,
    isp = 0,
    brand = "",
    model = "",
    uid = "",
    ua = "",
    os = 0,
    screen_w = 0,
    screen_h = 0,
    sex = 0,
    age = 0,
    coin = 0,
    isshow = 0,
    show_timestamp = 0,
    show_network = 0,
    show_ip = "",
    isclick = 0,
    click_timestamp = 0,
    click_network = 0,
    click_ip = "",
    antispam_score = 0,
    antispam_rules = "",
    date = "",
    hour = ""
  )

  def parseSearchLog(txt: String): UnionLog = {
    var log = EmptyUnionLog
    val notice = Aslog.NoticeLogBody.parseFrom(decodeLog(txt).toArray)
    if (notice.getSearchid.length > 0) {
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
    log
  }

  def parseShowLog(txt: String): UnionLog = {
    var log = EmptyUnionLog
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      log = log.copy(
        searchid = body.getSearchId,
        isshow = 1,
        show_timestamp = body.getEventTimestamp
      )
    }
    log
  }

  def parseClickLog(txt: String): UnionLog = {
    var log = EmptyUnionLog
    val event = Protocol.Event.parseFrom(decodeLog(txt).toArray)
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
