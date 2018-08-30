package com.cpc.spark.streaming.parser

import com.cpc.spark.common.{Event, LogData, Ui}
import com.cpc.spark.streaming.tools.Utils
import eventprotocol.Protocol

object StreamingDataParserV2 {
  val None = (false, 0, "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

  //(isok,adsr,sid,date,hour,typed, idea_id, unit_id, plan_id,user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click)
  def streaming_data_parse(logdata: LogData): (Boolean, Int, String, String, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
    var isok = true
    var ts = 0L
    if (logdata.log.hasLogTimestamp()) {
      ts = logdata.log.getLogTimestamp
    }

    val (date, hour) = Utils.get_date_hour_from_time(ts)

    var map: data.Data.Log.Field.Map = null
    if (logdata.log.hasField() && logdata.log.getField.getMapCount > 0) {
      map = logdata.log.getField.getMap(0)
    }
    if (ts == 0 || map == null) {
      None
    } else {
      val key = map.getKey
      val value = map.getValue.getStringType
      if (key.startsWith("cpc_search")) {
        val ui = Ui.parseData(value)
        if (ui == null) {
          None
        } else {
          val sid = ui.ui.getSearchid
          var adSrc = 0
          val ad = ui.ui.getAdsList
          var dspMediaId = ""
          var dspAdslotId = ""
          var idea_id = 0
          var unit_id = 0
          var plan_id = 0
          var user_id = 0
          var isfill = 0
          if (ad.size() > 0) {

            if (ui.ui.getAds(0).getSrc.getNumber == aslog.Aslog.AdSrc.CPC.getNumber) {
              idea_id = ui.ui.getAds(0).getAdid
              unit_id = ui.ui.getAds(0).getGroupid
              plan_id = ui.ui.getAds(0).getPlanid
              user_id = ad.get(0).getUserid
            } else {
              dspMediaId = ui.ui.getAds(0).getDspMediaId
              dspAdslotId = ui.ui.getAds(0).getDspAdslotId
            }
            adSrc = ui.ui.getAds(0).getSrc.getNumber
            isfill = 1
          } else {
            adSrc = switchSearchAdSrc(aslog.Aslog.AdSrc.CPC)
          }
          val price = 0
          val isdebug = ui.ui.getDebug
          if (isdebug) {
            isok = false
          }
          val media_id_str = ui.ui.getMedia.getAppsid
          var media_id = 0
          if (media_id_str.trim() != "") {
            media_id = media_id_str.toInt
          }
          val adslot_id_str = ui.ui.getAdslot(0).getId
          var adslot_id = 0
          if (adslot_id_str.trim() != "") {
            adslot_id = adslot_id_str.toInt
          }

          val adslot_type = ui.ui.getAdslot(0).getType.getNumber

          (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, ui.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 1, isfill, 0, 0)
        }
      } else if (key.startsWith("cpc_show")) {
        val show = Event.parse_show_log(value)
        if (show == null) {
          None
        } else {
          if (show.event.getAd.getType == eventprotocol.Protocol.Event.Body.AdType.AD_VIDEO && show.event.getShowTime != 0) {
            return None
          } else {
            val sid = show.event.getSearchId
            val adSrc = switchEventAdSrc(show.event.getDspInfo.getDsp)
            var dspMediaId = ""
            var dspAdslotId = ""
            var idea_id = 0
            var unit_id = 0
            var plan_id = 0
            var user_id = 0

            if (adSrc == 1) {
              idea_id = show.event.getAd.getUnitId
              unit_id = show.event.getAd.getGroupId
              plan_id = show.event.getAd.getPlanId
              user_id = show.event.getAd.getUserId
            } else {
              dspMediaId = show.event.getDspInfo.getMediaId
              dspAdslotId = show.event.getDspInfo.getAdslotId
              /*println("searchid:"+ show.event.getSearchId)
              println("src:"+ show.event.getDspInfo.getDsp)
              println("log:"+ value)*/
            }

            val price = 0
            val isdebug = false
            val media_id_str = show.event.getMedia.getMediaId
            var media_id = 0
            if (media_id_str.trim() != "") {
              media_id = media_id_str.toInt
            }

            val adslot_id_str = show.event.getMedia.getAdslotId
            var adslot_id = 0
            if (adslot_id_str.trim() != "") {
              adslot_id = adslot_id_str.toInt
            }
            val adslot_type = show.event.getMedia.getAdslotType.getNumber
            (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, show.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 0, 0, 1, 0)
          }
        }
      } else if (key.startsWith("cpc_click")) {
        val click = Event.parse_click_log(value)
        if (click == null) {
          None
        } else {
          val sid = click.event.getBody.getSearchId
          val adSrc = switchEventAdSrc(click.event.getBody.getDspInfo.getDsp)
          var dspMediaId = ""
          var dspAdslotId = ""
          /* val idea_id = click.event.getBody.getAd.getUnitId
           val unit_id = click.event.getBody.getAd.getGroupId
           val plan_id = click.event.getBody.getAd.getPlanId
           val user_id = click.event.getBody.getAd.getUserId*/
          var idea_id = 0
          var unit_id = 0
          var plan_id = 0
          var user_id = 0

          if (adSrc == 1) {
            idea_id = click.event.getBody.getAd.getUnitId
            unit_id = click.event.getBody.getAd.getGroupId
            plan_id = click.event.getBody.getAd.getPlanId
            user_id = click.event.getBody.getAd.getUserId
          } else {
            dspMediaId = click.event.getBody.getDspInfo.getMediaId
            dspAdslotId = click.event.getBody.getDspInfo.getAdslotId
          }

          val charge_type = click.event.getBody.getCharge.getType
          val price = if (charge_type == Protocol.ChargeType.CPC) {
            click.event.getBody.getCharge.getPrice
          } else 0

          val isdebug = false
          val media_id_str = click.event.getBody.getMedia.getMediaId
          var media_id = 0
          if (media_id_str.trim() != "") {
            media_id = media_id_str.toInt
          }
          val adslot_id_str = click.event.getBody.getMedia.getAdslotId
          var adslot_id = 0
          if (adslot_id_str.trim() != "") {
            adslot_id = adslot_id_str.toInt
          }
          val adslot_type = click.event.getBody.getMedia.getAdslotType.getNumber
          val score = click.event.getBody.getAntispam.getScore
          if (score <= 0) {
            isok = false
          }
          (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, click.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 0, 0, 0, 1)
        }
      } else {
        None
      }
    }
  }

  def switchEventAdSrc(idx: eventprotocol.Protocol.Dsp): Int = {
    var res: Int = 1
    idx match {
      case eventprotocol.Protocol.Dsp.CPC_DSP => res = 1;
      case eventprotocol.Protocol.Dsp.INMOBI_DSP => res = 2;
      case eventprotocol.Protocol.Dsp.TANX_SSP_DSP => res = 3;
      case eventprotocol.Protocol.Dsp.BOTTOMING => res = 4;
      case eventprotocol.Protocol.Dsp.HUZHONG_DSP => res = 5;
      case eventprotocol.Protocol.Dsp.FANCY_DSP => res = 6;
      case eventprotocol.Protocol.Dsp.GDT_DSP => res = 7;
      case _ => res = idx.getNumber;
    }
    res
  }

  def switchSearchAdSrc(idx: aslog.Aslog.AdSrc): Int = {
    var res: Int = 1
    idx match {
      case aslog.Aslog.AdSrc.CPC => res = 1;
      case aslog.Aslog.AdSrc.INMOBI => res = 2;
      case aslog.Aslog.AdSrc.TANXSSP => res = 3;
      case aslog.Aslog.AdSrc.BOTTOMINIG => res = 4;
      case aslog.Aslog.AdSrc.HUZHONG => res = 5;
      case aslog.Aslog.AdSrc.FANCY => res = 6;
      case aslog.Aslog.AdSrc.GDT => res = 7;
      case _ => res = idx.getNumber;
    }
    res
  }

  //临时刷数据2018-08-30 19:00:00 补计费失败数据
  //从src_clcik解析数据
  def parse_src_click(date: String, hour: Int, value: String): (Boolean, Int, String, String, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
    var isok = true
    val click = Event.parse_click_log(value)
    if (click == null) {
      None
    } else {
      val sid = click.event.getBody.getSearchId
      val adSrc = switchEventAdSrc(click.event.getBody.getDspInfo.getDsp)
      var dspMediaId = ""
      var dspAdslotId = ""

      var idea_id = 0
      var unit_id = 0
      var plan_id = 0
      var user_id = 0

      if (adSrc == 1) {
        idea_id = click.event.getBody.getAd.getUnitId
        unit_id = click.event.getBody.getAd.getGroupId
        plan_id = click.event.getBody.getAd.getPlanId
        user_id = click.event.getBody.getAd.getUserId
      } else {
        dspMediaId = click.event.getBody.getDspInfo.getMediaId
        dspAdslotId = click.event.getBody.getDspInfo.getAdslotId
      }

      val charge_type = click.event.getBody.getCharge.getType
      val price = if (charge_type == Protocol.ChargeType.CPC) {
        click.event.getBody.getCharge.getPrice
      } else 0

      val isdebug = false
      val media_id_str = click.event.getBody.getMedia.getMediaId
      var media_id = 0
      if (media_id_str.trim() != "") {
        media_id = media_id_str.toInt
      }
      val adslot_id_str = click.event.getBody.getMedia.getAdslotId
      var adslot_id = 0
      if (adslot_id_str.trim() != "") {
        adslot_id = adslot_id_str.toInt
      }
      val adslot_type = click.event.getBody.getMedia.getAdslotType.getNumber
      val score = click.event.getBody.getAntispam.getScore
      if (score <= 0) {
        isok = false
      }
      (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, click.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 0, 0, 0, 1)
    }

  }
}