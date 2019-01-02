
package com.cpc.spark.log.parser

import java.net.{InetAddress, URLDecoder}
import java.text.SimpleDateFormat
import java.util.Date

import aslog.Aslog
import aslog.Aslog.Adslot
import com.cpc.spark.common.{Cfg, Event, Ui}
import com.cpc.spark.ml.common.DspData.AdInfo
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
      val extInt = mutable.Map[String, Long]()
      val extString = mutable.Map[String, String]()
      val extFloat = mutable.Map[String, Double]()
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
        notice.getClient.getVersion.getMinor, notice.getClient.getVersion.getMicro, notice.getClient.getVersion.getBuild)))
      ext.update("media_site_url", ExtValue(string_value = notice.getMedia.getSite.getUrls))
      ext.update("client_requestId", ExtValue(string_value = notice.getClient.getRequestId))
      ext.update("client_isValid", ExtValue(int_value = if (notice.getClient.getIsValid) {
        1
      } else 0))


      var devices = ""
      val deviceCount = notice.getDevice.getIdsCount - 1
      for (i <- 0 to deviceCount) {
        if (devices.length > 0) {
          devices += ";" + notice.getDevice.getIds(i).getType + ":" + notice.getDevice.getIds(i).getId
        } else {
          devices += notice.getDevice.getIds(i).getType + ":" + notice.getDevice.getIds(i).getId
        }
      }
      extInt.update("browser_type", notice.getDevice.getBrowser.getNumber.toLong)

      ext.update("device_ids", ExtValue(string_value = devices))
      if (notice.getAdslotCount > 0) {
        val slot = notice.getAdslot(0)
        log = log.copy(
          adslotid = slot.getId,
          adslot_type = slot.getType.getNumber,
          floorbid = slot.getFloorbid,
          cpmbid = slot.getCpmbid
        )

        extInt.update("exp_style", slot.getExpStyle.toLong)
        extString.update("exp_feature", slot.getExpFeature)
        ext.update("channel", ExtValue(string_value = slot.getChannel))
        ext.update("pagenum", ExtValue(int_value = slot.getPagenum))
        ext.update("bookid", ExtValue(string_value = slot.getBookid))
      }
      if (notice.getDspReqInfoCount > 0) {
        val dspnum = notice.getDspReqInfoCount
        extInt.update("dsp_num", dspnum)
        for (i <- 0 until dspnum) {
          val dsp = notice.getDspReqInfo(i)
          val src = dsp.getSrc.getNumber
          extInt.update("dsp_src_%d".format(i), src)
          extString.update("dsp_mediaid_%d".format(i), dsp.getDspmediaid)
          extString.update("dsp_adslotid_%d".format(i), dsp.getDspadslotid)

          extString.update("dsp_adslotid_by_src_%d".format(src), dsp.getDspadslotid)

          val adnum = dsp.getRetAdsNum
          extInt.update("dsp_adnum_%d".format(i), adnum)
          extInt.update("dsp_adnum_by_src_%d".format(src), adnum)
        }
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

        extInt.update("siteid", ad.getSiteid.toLong)
        extInt.update("cvr_threshold_by_user", ad.getCvrThreshold)
        extInt.update("lastcpm", ad.getLastcpm)
        ext.update("click_unit_count", ExtValue(int_value = ad.getClickCount))
        ext.update("material_level", ExtValue(int_value = ad.getMaterialLevel.getNumber))
        ext.update("cvr_threshold", ExtValue(int_value = ad.getCvrThres.toInt))
        ext.update("exp_ctr", ExtValue(int_value = ad.getTitlectr.toInt))
        ext.update("exp_cvr", ExtValue(int_value = ad.getCvr.toInt))
        ext.update("adclass", ExtValue(int_value = ad.getClass_))
        ext.update("usertype", ExtValue(int_value = ad.getUsertype))
        ext.update("trigger_type", ExtValue(int_value = ad.getTriggerType))
        ext.update("rank_discount", ExtValue(int_value = ad.getDiscount))
        ext.update("user_req_ad_num", ExtValue(int_value = ad.getShowCount))
        ext.update("long_click_count", ExtValue(int_value = ad.getLongClickCount))
        ext.update("cvr_real_bid", ExtValue(int_value = ad.getRealBid))
        ext.update("adid_str", ExtValue(string_value = ad.getAdidStr))
        ext.update("ad_title", ExtValue(string_value = ad.getTitle))
        ext.update("ad_desc", ExtValue(string_value = ad.getDesc))
        ext.update("ad_img_urls", ExtValue(string_value = ad.getImgUrlsList.toArray.mkString(" ")))
        ext.update("ad_click_url", ExtValue(string_value = ad.getClickUrl))

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

      val siteInfo = notice.getMedia.getSite
      extString.update("title", siteInfo.getTitle)
      extString.update("config", siteInfo.getConfig)
      extString.update("hostname", notice.getHostname)
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
      ext.update("click_count", ExtValue(int_value = user.getClickCount)) //zycc
      ext.update("antispam", ExtValue(int_value = user.getAntispam))
      ext.update("share_coin", ExtValue(int_value = user.getShareCoin))
      ext.update("qukan_new_user", ExtValue(int_value = user.getNewuser))
      ext.update("user_req_num", ExtValue(int_value = user.getReqCount))
      extString.update("user_province", user.getProvince)
      extString.update("user_city", user.getCity)
      log = log.copy(
        sex = user.getSex,
        age = user.getAge,
        coin = user.getCoin,
        interests = interRows.mkString(","),
        ext = ext.toMap,
        ext_int = extInt.toMap,
        ext_string = extString.toMap,
        ext_float = extFloat.toMap
      )
    }
    log
  }

  def parseSearchLog_v2(txt: String): Seq[UnionLog] = {
    var logs = Seq[UnionLog]()
    var log: UnionLog = null
    val srcData = Ui.parseData(txt)
    if (srcData != null) {
      val notice = srcData.ui
      val (date, hour) = getDateHourFromTime(notice.getTimestamp)
      val ext = mutable.Map[String, ExtValue]()
      val extInt = mutable.Map[String, Long]()
      val extString = mutable.Map[String, String]()
      val extFloat = mutable.Map[String, Double]()
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
        notice.getClient.getVersion.getMinor, notice.getClient.getVersion.getMicro, notice.getClient.getVersion.getBuild)))
      ext.update("media_site_url", ExtValue(string_value = notice.getMedia.getSite.getUrls))
      ext.update("client_requestId", ExtValue(string_value = notice.getClient.getRequestId))
      ext.update("client_isValid", ExtValue(int_value = if (notice.getClient.getIsValid) {
        1
      } else 0))


      var devices = ""
      val deviceCount = notice.getDevice.getIdsCount - 1
      for (i <- 0 to deviceCount) {
        if (devices.length > 0) {
          devices += ";" + notice.getDevice.getIds(i).getType + ":" + notice.getDevice.getIds(i).getId
        } else {
          devices += notice.getDevice.getIds(i).getType + ":" + notice.getDevice.getIds(i).getId
        }
      }
      extInt.update("browser_type", notice.getDevice.getBrowser.getNumber.toLong)

      ext.update("device_ids", ExtValue(string_value = devices))
      if (notice.getAdslotCount > 0) {
        val slot = notice.getAdslot(0)
        log = log.copy(
          adslotid = slot.getId,
          adslot_type = slot.getType.getNumber,
          floorbid = slot.getFloorbid,
          cpmbid = slot.getCpmbid
        )

        extInt.update("exp_style", slot.getExpStyle.toLong)
        extString.update("exp_feature", slot.getExpFeature)
        ext.update("channel", ExtValue(string_value = slot.getChannel))
        ext.update("pagenum", ExtValue(int_value = slot.getPagenum))
        ext.update("bookid", ExtValue(string_value = slot.getBookid))
      }
      if (notice.getDspReqInfoCount > 0) {
        val dspnum = notice.getDspReqInfoCount
        extInt.update("dsp_num", dspnum)
        for (i <- 0 until dspnum) {
          val dsp = notice.getDspReqInfo(i)
          val src = dsp.getSrc.getNumber
          extInt.update("dsp_src_%d".format(i), src)
          extString.update("dsp_mediaid_%d".format(i), dsp.getDspmediaid)
          extString.update("dsp_adslotid_%d".format(i), dsp.getDspadslotid)

          extString.update("dsp_adslotid_by_src_%d".format(src), dsp.getDspadslotid)

          val adnum = dsp.getRetAdsNum
          extInt.update("dsp_adnum_%d".format(i), adnum)
          extInt.update("dsp_adnum_by_src_%d".format(src), adnum)
        }
      }

      if (notice.getAdsCount > 0) {
        val ad = notice.getAds(0)
        extString.update("downloaded_app", ad.getAppInfo.getName)
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
          cpm = ad.getCpm,
          ext_string = extString
        )

        extInt.update("siteid", ad.getSiteid.toLong)
        extInt.update("cvr_threshold_by_user", ad.getCvrThreshold)
        extInt.update("lastcpm", ad.getLastcpm)
        ext.update("click_unit_count", ExtValue(int_value = ad.getClickCount))
        ext.update("material_level", ExtValue(int_value = ad.getMaterialLevel.getNumber))
        ext.update("cvr_threshold", ExtValue(int_value = ad.getCvrThres.toInt))
        ext.update("exp_ctr", ExtValue(int_value = ad.getTitlectr.toInt))
        ext.update("exp_cvr", ExtValue(int_value = ad.getCvr.toInt))
        ext.update("adclass", ExtValue(int_value = ad.getClass_))
        ext.update("usertype", ExtValue(int_value = ad.getUsertype))
        ext.update("trigger_type", ExtValue(int_value = ad.getTriggerType))
        ext.update("rank_discount", ExtValue(int_value = ad.getDiscount))
        ext.update("user_req_ad_num", ExtValue(int_value = ad.getShowCount))
        ext.update("long_click_count", ExtValue(int_value = ad.getLongClickCount))
        ext.update("cvr_real_bid", ExtValue(int_value = ad.getRealBid))
        ext.update("adid_str", ExtValue(string_value = ad.getAdidStr))
        ext.update("ad_title", ExtValue(string_value = ad.getTitle))
        ext.update("ad_desc", ExtValue(string_value = ad.getDesc))
        ext.update("ad_img_urls", ExtValue(string_value = ad.getImgUrlsList.toArray.mkString(" ")))
        ext.update("ad_click_url", ExtValue(string_value = ad.getClickUrl))

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

      val siteInfo = notice.getMedia.getSite
      extString.update("title", siteInfo.getTitle)
      extString.update("config", siteInfo.getConfig)

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
      ext.update("click_count", ExtValue(int_value = user.getClickCount)) //zycc
      ext.update("antispam", ExtValue(int_value = user.getAntispam))
      ext.update("share_coin", ExtValue(int_value = user.getShareCoin))
      ext.update("qukan_new_user", ExtValue(int_value = user.getNewuser))
      ext.update("user_req_num", ExtValue(int_value = user.getReqCount))
      extString.update("user_province", user.getProvince)
      extString.update("user_city", user.getCity)
      extString.update("qtt_member_id", user.getMemberId)
      extString.update("hostname", notice.getHostname)
      extString.update("exp_ids", notice.getAbtestIdsList.toArray().mkString(","))
      extInt.update("lx_type", user.getLxType.toLong)
      extInt.update("lx_package", user.getLxPackage.toLong)
      log = log.copy(
        sex = user.getSex,
        age = user.getAge,
        coin = user.getCoin,
        interests = interRows.mkString(","),
        ext = ext.toMap,
        ext_int = extInt.toMap,
        ext_string = extString.toMap,
        ext_float = extFloat.toMap
      )
      logs = logs :+ log
      if (notice.getAdsCount > 1 && notice.getAdslot(0).getType.getNumber == 7) {
        for (i <- 1 until notice.getAdsList.size()) {
          val ad = notice.getAds(i)
          extString.update("downloaded_app", ad.getAppInfo.getName)
          logs = logs :+ log.copy(
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
            cpm = ad.getCpm,
            ext_string = extString.toMap
          )
        }
      }
    }
    logs
  }

  def parseShowLog(txt: String): UnionLog = {
    //zyc
    var log: UnionLog = null
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      val ext = mutable.Map[String, ExtValue]()
      ext.update("show_refer", ExtValue(string_value = data.refer))
      ext.update("show_ua", ExtValue(string_value = data.ua))
      ext.update("video_show_time", ExtValue(int_value = body.getShowTime)) //video_show_time
      ext.update("charge_type", ExtValue(int_value = body.getCharge.getType.getNumber))
      log = UnionLog(
        searchid = body.getSearchId,
        isshow = 1,
        ideaid = body.getAd.getUnitId,
        show_timestamp = data.timestamp,
        show_ip = data.ip,
        ext = ext
      )
    }
    log
  }

  /**
    * 解析 Show log;
    * 将ext拆开;
    *
    * @param txt 序列化的日志
    * @return ParsedShowLog对象
    */
  def parseShowLog_v2(txt: String): ShowLog = {
    //zyc
    var log: ShowLog = null
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event

      //      val ext = mutable.Map[String, ExtValue]()
      //      ext.update("show_refer", ExtValue(string_value = data.refer))
      //      ext.update("show_ua", ExtValue(string_value = data.ua))
      //      ext.update("video_show_time", ExtValue(int_value = body.getShowTime)) //video_show_time
      //      ext.update("charge_type", ExtValue(int_value = body.getCharge.getType.getNumber))

      log = ShowLog(
        searchid = body.getSearchId,
        isshow = 1,
        ideaid = body.getAd.getUnitId,
        show_timestamp = data.timestamp,
        show_ip = data.ip,
        show_refer = data.refer,
        show_ua = data.ua,
        video_show_time = body.getShowTime,
        charge_type = body.getCharge.getType.getNumber,
        show_network = body.getNetwork.getType.getNumber

        //        ext=ext
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
      if (aid.length <= 0) {
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
        hour = hour,
        ip = body.getIp,
        ua = body.getUa
      )
    }
    log
  }

  /**
    * 添加deviceid字段  2018-10-29
    *
    * @param txt
    * @return
    */
  def parseCfgLog_v2(txt: String): CfgLog = {
    var log: CfgLog = null
    val data = Cfg.parseData(txt)
    if (data != null) {
      val body = data.cfg
      var aid = body.getAdSlotId
      val (date, hour) = getDateHourFromTime(body.getTimestamp)
      if (aid.length <= 0) {
        aid = body.getAidList.toArray().mkString(",")
      }

      var deviceId = ""
      val formCount = body.getForm.getValuesCount
      if (formCount > 0) {
        for (i <- 0 until formCount) {
          if (body.getForm.getValues(i).getKey == "dc") {
            deviceId = body.getForm.getValues(i).getValue
          }
        }
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
        hour = hour,
        ip = body.getIp,
        ua = body.getUa,
        deviceid = deviceId
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
          ideaid = body.getAd.getUnitId,
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

  /**
    * 解析Click log
    *
    * @param txt 序列化的日志
    * @return ParsedClickLog对象
    */
  def parseClickLog_v2(txt: String): ClickLog = {
    var log: ClickLog = null
    val data = Event.parse_click_log(txt)
    if (data != null) {
      val event = data.event
      if (event.getBody.getSearchId.length > 0) {
        val body = event.getBody
        val extra = event.getExtra
        //        val ext = mutable.Map[String, ExtValue]()
        //        ext.update("touch_x", ExtValue(int_value = extra.getTouchX))
        //        ext.update("touch_y", ExtValue(int_value = extra.getTouchY))
        //        ext.update("slot_width", ExtValue(int_value = extra.getWidth))
        //        ext.update("slot_height", ExtValue(int_value = extra.getHeight))
        //        ext.update("antispam_predict", ExtValue(float_value = body.getAntispam.getPredict))
        //        ext.update("click_ua", ExtValue(string_value = body.getAction.getUserAgent))
        log = ClickLog(
          searchid = body.getSearchId,
          isclick = 1,
          ideaid = body.getAd.getUnitId,
          click_timestamp = body.getEventTimestamp,
          antispam_score = body.getAntispam.getScore,
          antispam_rules = body.getAntispam.getRulesList.toArray.mkString(","),
          click_ip = LongToIPv4(body.getEventIp.toLong),
          touch_x = extra.getTouchX,
          touch_y = extra.getTouchY,
          slot_width = extra.getWidth,
          slot_height = extra.getHeight,
          antispam_predict = body.getAntispam.getPredict,
          click_ua = body.getAction.getUserAgent,
          click_network = body.getNetwork.getType.getNumber

          //          ext = ext.toMap
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
          click_ip = LongToIPv4(body.getEventIp.toLong),
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
          adsrc = body.getDspInfo.getDsp.getNumber,
          ext = ext.toMap
        )
      }
    }
    log
  }

  //val txt = "36.149.39.90 - - [15/May/2017:08:02:36 +0800] \"GET /trace?t=stay&duration=1&iclicashsid=90c60d5e0fc887090984f5589aaa157a62207613&w=980&h=1306&sw=360&sh=640&os=Android&ref=http%3A%2F%2Fcj.juseyx.com%2Fb2%2F%3Ficlicashsid%3D90c60d5e0fc887090984f5589aaa157a62207613&v=1.0&_t=0 HTTP/1.1\" 200 43 \"http://cj.juseyx.com/b2/?iclicashsid=90c60d5e0fc887090984f5589aaa157a62207613&t=1494806555248\" \"Mozilla/5.0 (Linux; Android 6.0; NEM-TL00 Build/HONORNEM-TL00; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36 qukan_android\" \"-\" \"-\" 0.000"

  //        "118.121.245.117 - - [09/Apr/2018:01:50:00 +0800] ""GET /trace?t=inter_click&adslot_id=7409447 HTTP/1.1"" 200 43 ""http://cdn.aiclicash.com/game/directory/directory.html?iclicashid=7409447&&settings=1&&gameType=48,47,46,45,44,43&&gameTimes=8&&isFull=0&&rate=0&&back=0&&redpack=1&&appShow=0"" ""Mozilla/5.0 (Linux; Android 6.0; KONKA D6+ Build/MRA58K; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/48.0.2531.0 Mobile Safari/537.36"" ""-"" ""-"" 0.000",1523212322086,50238-09-05 13:54:46,"39.179.56.224 - - [09/Apr/2018:02:32:00 +0800] ""GET /trace?t=inter_click&adslot_id=7409447 HTTP/1.1"" 200 43 ""


  val traceRegex = """GET\s/trace\?([^\s]+)""".r


  def parseTraceLog(txt: String): TraceLog = {
    var log: TraceLog = null
    if (txt != null) {
      val txt2 = txt.substring(0, 30)
      val traceRegex2 = """(\d{1,3}\.){3}\d{1,3}""".r
      val ip = traceRegex2.findFirstMatchIn(txt2).getOrElse("").toString
      traceRegex.findFirstMatchIn(txt).foreach {
        m =>
          val sub = m.subgroups
          if (sub.length == 1) {
            log = TraceLog()
            val opt = mutable.Map[String, String]();
            sub(0).split('&').foreach {
              x =>
                try {
                  val Array(k, vv) = x.trim.split("=", 2)
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
                    case "adslot_id" => log = log.copy(adslot_id = toInt(v))
                    case "ua" => log = log.copy(ua = v)
                    case s if s.startsWith("opt_") =>
                      val key = s.stripPrefix("opt_")
                      if (key.length > 0) {
                        opt.update(key, v.trim)
                      }
                    case _ =>
                  }
                } catch {
                  case e: Exception => null
                }
            }
            log = log.copy(ip = ip, opt = opt.toMap)
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
                  val Array(k, vv) = x.trim.split("=", 2)
                  val v = URLDecoder.decode(vv.trim, "UTF8")
                  k match {
                    case "t" => log = log.copy(log_type = v)
                    case "adslot_id" => log = log.copy(adslot_id = toInt(v))
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

  def LongToIPv4(ip: Long): String = {
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
      case e: Exception => 0
    }
  }

  def toAutoInt(s: String): Int = {
    try {
      if (s.indexOf("1") >= 0) {
        return 1
      } else {
        return s.trim.toInt
      }
    } catch {
      case e: Exception => 0
    }
  }

  def toFloat(s: String): Float = {
    try {
      s.trim.toFloat
    } catch {
      case e: Exception => 0
    }
  }
}


