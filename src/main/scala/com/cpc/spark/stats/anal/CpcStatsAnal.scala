package com.cpc.spark.stats.anal

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import com.cpc.spark.common.Ui
import com.cpc.spark.common.Event
import org.apache.spark.sql.types._

object CpcStatsAnal {
  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println(s"""
        |Usage: CpcStatsAnal <hdfs_input> <hdfs_output> <date> <hour>
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(input, output, date, hour) = args

    val spark = SparkSession.builder()
      .appName("SparkSQL Anal date=" + date + "/" + hour)
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._
    val search_input = input + "/cpc_search/" + date + "/" + hour
    val show_input = input + "/cpc_show/" + date + "/" + hour
    val click_input = input + "/cpc_click/" + date + "/" + hour
    val charge_input = input + "/cpc_charge/" + date + "/" + hour


    val fields = Array(
        StructField("log_timestamp",LongType,true),
        StructField("ip",StringType,true),
        StructField("field",MapType(StringType,
            StructType(Array(
                StructField("int_type",IntegerType,true),
                StructField("long_type",LongType,true),
                StructField("float_type",FloatType,true),
                StructField("string_type",StringType,true))),true),true))
    
    val schema= StructType(fields)
         
    val showBaseData = spark.read.schema(schema).parquet(show_input)
    val clickBaseData = spark.read.schema(schema).parquet(click_input)
    val searchBaseData = spark.read.schema(schema).parquet(search_input)
    val chargeBaseData = spark.read.schema(schema).parquet(charge_input)
   
    searchBaseData.createTempView("search_data")
    showBaseData.createTempView("show_data")
    clickBaseData.createTempView("click_data")
    chargeBaseData.createTempView("charge_data")

    val AS_None = ((0, 0, 0, 0, 0, 0, 0, 0, 0, "", date, hour), (0, 0, 0, 0, 0, 0, 0, 0))
    val None = ((0, 0, 0, 0, 0, 0, 0, 0, 0, date, hour), (0, 0, 0, 0, 0, 0, 0, 0))

    val searchRDD = spark.sql("select field['cpc_search'].string_type from search_data").rdd
    val search_data = searchRDD.map { x =>
      val xx = x.getString(0)
      val search = Ui.parseData(xx)
      if (search == null) {
        AS_None
      } else {
        val media_id_str = search.ui.getMedia.getAppsid
        var media_id = 0
        if (media_id_str.trim() != "") {
          media_id = media_id_str.toInt
        }
        val adslot_id_str = search.ui.getAdslot(0).getId
        var adslot_id = 0
        if (adslot_id_str.trim() != "") {
          adslot_id = adslot_id_str.toInt
        }
        var isfill = 0
        var idea_id = 0
        var unit_id = 0
        var plan_id = 0
        if (search.ui.getAdsCount > 0) {
          isfill = 1
          val ad = search.ui.getAds(0)
          idea_id = ad.getAdid
          unit_id = ad.getGroupid
          plan_id = ad.getPlanid
        }
        val os_id = search.ui.getDevice.getOs.getNumber
        val country = search.ui.getLocation.getCountry
        val province = search.ui.getLocation.getProvince
        val city = search.ui.getLocation.getCity

        val uid = search.ui.getDevice.getUid

        val isdebug = search.ui.getDebug
        if (isdebug) {
          ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, uid, date, hour), (0, 0, 0, 0, 0, 0, 0, 0))
        } else {
          ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, uid, date, hour), (1, isfill, 0, 0, 0, 0, 0, 0))
        }
      }
    }.cache()

    val showRDD = spark.sql("select field['cpc_show'].string_type from show_data").rdd
    val show_data = showRDD.map { x =>
      val xx = x.getString(0)
      val show = Event.parse_show_log(xx)
      if (show == null) {
        println(x)
        None
      } else {
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
        val idea_id = show.event.getAd.getUnitId
        val unit_id = show.event.getAd.getGroupId
        val plan_id = show.event.getAd.getPlanId

        val os_id = show.event.getDevice.getOs.getNumber
        val country = show.event.getEventLocation.getCountry
        val province = show.event.getEventLocation.getProvince
        val city = show.event.getEventLocation.getCity

        ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, date, hour), (0, 0, 1, 0, 0, 0, 0, 0))
      }
    }.cache()

    val clickRDD = spark.sql("select field['cpc_click'].string_type from click_data").rdd
    val click_data = clickRDD.map { x =>
      val xx = x.getString(0)
      val click = Event.parse_click_log(xx)
      if (click == null) {
        None
      } else {
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
        val idea_id = click.event.getBody.getAd.getUnitId
        val unit_id = click.event.getBody.getAd.getGroupId
        val plan_id = click.event.getBody.getAd.getPlanId
        val score = click.event.getBody.getAntispam.getScore

        val os_id = click.event.getBody.getDevice.getOs.getNumber
        val country = click.event.getBody.getEventLocation.getCountry
        val province = click.event.getBody.getEventLocation.getProvince
        val city = click.event.getBody.getEventLocation.getCity
        var cash_cost = 0
        var coupon_cost = 0
        if (score > 0) {
          cash_cost = click.event.getBody.getCharge.getPrice
          coupon_cost = 0
          ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, date, hour), (0, 0, 0, 1, 1, 0, cash_cost, coupon_cost))
        } else {
          ((media_id, adslot_id, idea_id, unit_id, plan_id, os_id, country, province, city, date, hour), (0, 0, 0, 0, 0, 1, cash_cost, coupon_cost))
        }
      }
    }.cache()
    //charge
    val charge_store_hourly = ChargeHourlyAnal.anal(search_data, show_data, click_data)
    val media_os_hourly_store = MediaOsHourlyAnal.anal(search_data, show_data, click_data)
    val media_geo_hourly_store = MediaGeoHourlyAnal.anal(search_data, show_data, click_data)
    val media_uv_hourly_store = MediaUVAnal.anal(search_data)
    
    charge_store_hourly.saveAsTextFile(output + "/media_charge_hourly")
    media_os_hourly_store.saveAsTextFile(output + "/media_os_hourly")
    media_geo_hourly_store.saveAsTextFile(output + "/media_geo_hourly")
    media_uv_hourly_store.saveAsTextFile(output + "/media_uv_hourly")
    
//        clickRDD.saveAsTextFile(output + "/clickRDD")
//        click_data.saveAsTextFile(output + "/click_data")
//        val chargeRDD = spark.sql("select field['media_id'].int_type,field['adslot_id'].int_type,field['idea_id'].int_type,field['unit_id'].int_type,field['plan_id'].int_type,field['click'].int_type,field['balance'].int_type,field['coupon'].int_type,field['sql_status'].int_type from charge_data").rdd
//        val charge_data = chargeRDD.map { x =>
//          try {
//            val media_id = x.getInt(0)
//            val adslot_id = x.getInt(1)
//            val idea_id = x.getInt(2)
//            val unit_id = x.getInt(3)
//            val plan_id = x.getInt(4)
//            val charge_click = x.getInt(5)
//            val balance = x.getInt(6)
//            val coupon = x.getInt(7)
//            val sql_status = x.getInt(8)
//            if (sql_status == 0) {
//              ((media_id, adslot_id, idea_id, unit_id, plan_id, date, hour), (0, 0, 0, 0, charge_click, 0, balance, coupon))
//            } else {
//              ((media_id, adslot_id, idea_id, unit_id, plan_id, date, hour), (0, 0, 0, 0, 0, 0, 0, 0))
//            }
//          } catch {
//            case ex: Exception =>
//              None
//          }
//        }
//        charge_data.saveAsTextFile(output + "/charge_data")
//
//        val union_data = search_data.union(show_data).union(click_data)
//        val media_charge_data = union_data.reduceByKey {
//          case (x, y) =>
//            (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6, x._7 + y._7, x._8 + y._8)
//        }.filter {
//          case ((media_id, adslot_id, idea_id, unit_id, plan_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
//            media_id != 0
//        }.map {
//          case ((media_id, adslot_id, idea_id, unit_id, plan_id, date, hour), (req, fill, imp, click, charge_click, spam_click, balance, coupon)) =>
//            media_id + "|" + adslot_id + "|" + idea_id + "|" + unit_id + "|" + plan_id + "|" + date + "|" + hour + "|" + req + "|" + fill + "|" + imp + "|" + click + "|" + charge_click + "|" + spam_click + "|" + balance + "|" + coupon
//        }
//        //    val d_data = w_data.rdd.map { x => x.getString(0)}
//        //    union_data.saveAsTextFile(output + "/union_data")
//        media_charge_data.saveAsTextFile(output)
  }
}