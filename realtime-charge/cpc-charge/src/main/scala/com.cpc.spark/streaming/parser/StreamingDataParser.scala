package com.cpc.spark.streaming.parser

import com.cpc.spark.common.LogData
import com.cpc.spark.common.Ui
import com.cpc.spark.common.Event
import com.cpc.spark.streaming.tools.Utils

object StreamingDataParser {
  val None = (false, "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
  //(isok,sid,date,hour,typed, idea_id, unit_id, plan_id,user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click)
  def streaming_data_parse(logdata: LogData): (Boolean, String, String, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int) = {
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
          val ad = ui.ui.getAdsList
          var idea_id = 0
          var unit_id = 0
          var plan_id = 0
          var user_id = 0
          var isfill = 0
          if (ad.size() > 0) {
            idea_id = ui.ui.getAds(0).getAdid
            unit_id = ui.ui.getAds(0).getGroupid
            plan_id = ui.ui.getAds(0).getPlanid
            user_id = ad.get(0).getUserid
            isfill = 1
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

          (isok, sid, date, hour, ui.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 1, isfill, 0, 0)
        }
      } else if (key.startsWith("cpc_show")) {
        val show = Event.parse_show_log(value)
        if (show == null) {
          None
        } else {
          val sid = show.event.getSearchId
          val idea_id = show.event.getAd.getUnitId
          val unit_id = show.event.getAd.getGroupId
          val plan_id = show.event.getAd.getPlanId
          val user_id = show.event.getAd.getUserId
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
          (isok, sid, date, hour, show.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 0, 0, 1, 0)
        }
      } else if (key.startsWith("cpc_click")) {
        val click = Event.parse_click_log(value)
        if (click == null) {
          None
        } else {
          val sid = click.event.getBody.getSearchId
          val idea_id = click.event.getBody.getAd.getUnitId
          val unit_id = click.event.getBody.getAd.getGroupId
          val plan_id = click.event.getBody.getAd.getPlanId
          val user_id = click.event.getBody.getAd.getUserId
          val price = click.event.getBody.getCharge.getPrice
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
          (isok, sid, date, hour, click.typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, 0, 0, 0, 1)
        }
      } else {
        None
      }
    }
  }

}