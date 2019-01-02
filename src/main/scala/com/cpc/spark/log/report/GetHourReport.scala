package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import breeze.linalg.sum
import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory
import eventprotocol.Protocol.ChargeType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetHourReport {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  var mariadb_amateur_url = ""
  val mariadb_amateur_prop = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetHourReport <hive_table> <date:string> <hour:string>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val table = args(0)
    val date = args(1)
    val hour = args(2)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    mariadb_amateur_url = conf.getString("mariadb.amateur_write.url")
    mariadb_amateur_prop.put("user", conf.getString("mariadb.amateur_write.user"))
    mariadb_amateur_prop.put("password", conf.getString("mariadb.amateur_write.password"))
    mariadb_amateur_prop.put("driver", conf.getString("mariadb.amateur_write.driver"))
    val ctx = SparkSession.builder()
      .appName("cpc get hour report from %s %s/%s".format(table, date, hour))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._


    val unionLog1 = ctx.sql(
      s"""
         |select *,
         |      ext['spam_click'].int_value as spam_click,
         |      ext['rank_discount'].int_value as rank_discount,
         |      ext['cvr_threshold'].int_value as cvr_threshold,
         |      ext['adclass'].int_value as adclass,
         |      ext['exp_cvr'].int_value as exp_cvr,
         |      ext['exp_ctr'].int_value as exp_ctr,
         |      if(ext["charge_type"].int_value=2,"cpm","cpc") as charge_type,
         |      if(ext["charge_type"].int_value=2,price/1000,price) as charge_fee
         |from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and isfill = 1 and adslotid > 0 and adsrc <= 1
       """.stripMargin.format(table, date, hour))
      .rdd
      .cache()

    val unionLog = unionLog1.filter(x => x.getAs[String]("charge_type") == "cpc")

    //激励广告数据（只加到charge表）
    val motive_data = ctx.sql(
      s"""
         |select m.unitid,m.planid,m.ideaid,m.userid,
         |       m.isfill,m.isshow,m.isclick,m.price as charge_fee,
         |       media_appsid,adslotid,adslot_type,"cpc" as charge_type,
         |       date,hour,0 as spam_click
         |from dl_cpc.cpc_union_log
         |lateral view explode(motivation) b as m
         |where date='$date' and hour=$hour
         |   and adslot_type=7
         |   and m.ideaid>=0
         |   and adsrc <= 1
        """.stripMargin)
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          val chargeType = x.getAs[String]("charge_type")
          var charge_fee = if (isclick > 0 || chargeType == "cpm")
            x.getAs[Int]("charge_fee")
          else 0D
          if (charge_fee > 10000 || charge_fee < 0) {
            charge_fee = 0
          }

          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          (charge.key, (charge, charge_fee))
      }.rdd

    val chargeData = unionLog1
      .filter(_.getAs[Int]("adslot_type") != 7)
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          val chargeType = x.getAs[String]("charge_type")
          var charge_fee = if (isclick > 0 || chargeType == "cpm")
            x.getAs[Double]("charge_fee")
          else 0D

          if (charge_fee > 10000 || charge_fee < 0) {
            charge_fee = 0
          }

          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          (charge.key, (charge, charge_fee))
      }
      .union(motive_data)
      .reduceByKey((x, y) => (x._1.sum(y._1), x._2 + y._2), 100)
      .map(x => x._2._1.copy(cash_cost = x._2._2.toInt))


    clearReportHourData("report_media_charge_hourly", date, hour)
    val chargedata = ctx.createDataFrame(chargeData).persist
    chargedata.write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_charge_hourly", mariadbProp)

    chargedata.write
      .mode(SaveMode.Append)
      .jdbc(mariadb_amateur_url, "report.report_media_charge_hourly", mariadb_amateur_prop)

    println("charge", chargeData.count())

    val geoData = unionLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          if (realCost > 10000 || realCost < 0) {
            realCost = 0
          }
          val report = MediaGeoReport(
            //media_id = x.media_appsid.toInt,
            //adslot_id = x.adslotid.toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            country = x.getAs[Int]("country"),
            province = x.getAs[Int]("province"),
            //city = x.city,
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_geo_hourly", date, hour)
    ctx.createDataFrame(geoData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_geo_hourly", mariadbProp)
    println("geo", geoData.count())

    val osData = unionLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          if (realCost > 10000 || realCost < 0) {
            realCost = 0
          }

          val report = MediaOsReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            os_type = x.getAs[Int]("os"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_os_hourly", date, hour)
    ctx.createDataFrame(osData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_os_hourly", mariadbProp)
    println("os", osData.count())

    val dsplog = ctx.sql(
      s"""
         |select *
         |from dl_cpc.$table
         |where date='$date' and hour='$hour'
      """.stripMargin)
    //val dsplog = ctx.read.parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, hour))
    val dspdata = dsplog.rdd
      .flatMap {
        x =>
          val isclick = x.getAs[Int]("isclick")
          var realCost = 0
          if (isclick > 0) {
            realCost = x.getAs[Int]("price")
          }
          if (realCost > 10000 || realCost < 0) {
            realCost = 0
          }

          val report = ReqDspReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            request = 1,
            date = "%s %s:00:00".format(date, hour)
          )
          val adsrc = x.getAs[Int]("adsrc").toLong

          val extInt = x.getAs[Map[String, Long]]("ext_int")
          val extString = x.getAs[Map[String, String]]("ext_string")
          val dspnum = extInt.getOrElse("dsp_num", 0L)
          var rows = Seq[ReqDspReport]()
          for (i <- 0 until dspnum.toInt) {
            val src = extInt.getOrElse("dsp_src_" + i, 0L)
            val mediaid = extString.getOrElse("dsp_mediaid_" + i, "")
            val adslotid = extString.getOrElse("dsp_adslotid_" + i, "")
            val adnum = extInt.getOrElse("dsp_adnum_" + i, 0L)

            val fill = if (src == adsrc) x.getAs[Int]("isfill") else 0
            val shows = if (src == adsrc) x.getAs[Int]("isshow") else 0
            val dsp_click = if (src == adsrc) isclick else 0
            val dsp_cash = if (src == adsrc) realCost else 0
            rows = rows :+ report.copy(
              dsp_src = src.toInt,
              dsp_mediaid = mediaid,
              dsp_adslotid = adslotid,
              dsp_adnum = adnum.toInt,
              fill = fill,
              shows = shows,
              click = dsp_click,
              cash_cost = dsp_cash
            )
          }
          rows
      }
      .map {
        x =>
          val key = (x.media_id, x.adslot_id, x.dsp_src, x.dsp_mediaid, x.dsp_adslotid, x.date)
          (key, x)
      }
      .reduceByKey((x, y) => x.sum(y), 20)
      .map(x => x._2)

    clearReportHourData2("report_req_dsp_hourly", date + " " + hour + ":00:00")
    ctx.createDataFrame(dspdata)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_req_dsp_hourly", mariadbProp)
    println("dsp", dspdata.count())

    val fillLog = ctx.sql(
      s"""
         |select *,
         |      ext['spam_click'].int_value as spam_click,
         |      ext['rank_discount'].int_value as rank_discount,
         |      ext['cvr_threshold'].int_value as cvr_threshold,
         |      ext['adclass'].int_value as adclass,
         |      ext['exp_cvr'].int_value as exp_cvr,
         |      ext['exp_ctr'].int_value as exp_ctr
         |      from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and adslotid > 0 and adsrc <= 1
         |      and (ext["charge_type"].int_value=1 or ext["charge_type"] is null)
           """.stripMargin.format(table, date, hour))
      //      .as[UnionLog]
      .rdd

    val fillData = fillLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          if (realCost > 10000 || realCost < 0) {
            realCost = 0
          }
          val report = MediaFillReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_fill_hourly", date, hour)
    ctx.createDataFrame(fillData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_fill_hourly", mariadbProp)
    println("fill", fillData.count())


    val unionLog_tmp = unionLog.filter(x => x.getAs[Int]("ideaid") > 0 && x.getAs[Int]("isshow") > 0).cache()

    //取展示top10 的adclass
    val topAdclass = unionLog_tmp
      .map(x => (x.getAs[Int]("adclass"), 1))
      .reduceByKey(_ + _)
      .sortBy(x => x._2, false)
      .map(x => x._1)
      .take(10)
      .toSeq
    println("topAdclass: " + topAdclass)

    val ctrData = unionLog_tmp
      .map {
        u =>
          val exptag = u.getAs[String]("exptags").split(",").find(_.startsWith("ctrmodel")).getOrElse("base")
          var expctr = u.getAs[Int]("exp_ctr")
          expctr = if (expctr < 0) 0 else expctr
          var isclick = u.getAs[Int]("isclick")
          var spam_click = u.getAs[Int]("spam_click")
          var antispam_score = u.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = u.getAs[Int]("price")
          } else {
            realCost = 0
          }
          if (realCost > 10000 || realCost < 0) {
            realCost = 0
          }

          var adclass = u.getAs[Int]("adclass")
          /*
          110110100  网赚
          130104101  男科
          125100100  彩票
          100101109  扑克
          99   其他
           */
          //val topAdclass = Seq(110110100, 130104101, 125100100, 100101109)
          if (!topAdclass.contains(adclass)) {
            adclass = 99
          }

          val cost = realCost.toFloat

          val ctr = CtrReport(
            media_id = u.getAs[String]("media_appsid").toInt,
            adslot_id = u.getAs[String]("adslotid").toInt,
            adslot_type = u.getAs[Int]("adslot_type"),
            adclass = adclass,
            exp_tag = exptag,
            request = 1,
            served_request = u.getAs[Int]("isfill"),
            impression = u.getAs[Int]("isshow"),
            cash_cost = cost,
            click = isclick,
            exp_click = expctr,
            date = "%s %s:00:00".format(u.getAs[String]("date"), u.getAs[String]("hour"))
          )
          (u.getAs[String]("searchid"), ctr)
      }
    unionLog_tmp.unpersist()

    //get cvr data
    val cvrlog = ctx.sql(
      //      s"""
      //         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" and hour = "%s"
      //            """.stripMargin.format(date, hour))
      s"""
         |select a.searchid as search_id
         |       ,a.adslot_type
         |       ,a.ext["client_type"].string_value as client_type
         |       ,a.ext["adclass"].int_value  as adclass
         |       ,a.ext_int['siteid'] as siteid
         |       ,a.adsrc
         |       ,a.interaction
         |       ,b.*
         |from (select * from dl_cpc.cpc_union_log
         |        where `date` = "%s" and `hour` = "%s" ) a
         |    left join (select id from bdm.cpc_userid_test_dim where day='%s') t2
         |         on a.userid = t2.id
         |    left join
         |        (select *
         |            from dl_cpc.cpc_union_trace_log
         |            where `date` = "%s" and `hour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid
         |where b.searchid is not null and t2.id is null
        """.stripMargin.format(date, hour, date, date, hour))
      .rdd
      .map {
        x =>
          (x.getAs[String]("searchid"), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = Utils.cvrPositiveV(x._2, "v2")
          val (convert2, label_type) = Utils.cvrPositiveV2(x._2, "v2") //新cvr,不包含用户回传api cvr
          (x._1, (convert, convert2))
        //(x._1, convert)
      }

    val ctrCvrData = ctrData.leftOuterJoin(cvrlog)
      //.map { x => x._2._1.copy(cvr_num = x._2._2.getOrElse(0)) }
      .map { x =>
      x._2._1.copy(cvr_num = x._2._2.getOrElse((0, 0))._1)
      x._2._1.copy(cvr2_num = x._2._2.getOrElse((0, 0))._2)
    }
      .map {
        ctr =>
          val key = (ctr.media_id, ctr.adslot_id, ctr.adclass, ctr.exp_tag)
          (key, ctr)
      }
      .reduceByKey {
        (x, y) =>
          x.copy(
            request = x.request + y.request,
            served_request = x.served_request + y.served_request,
            impression = x.impression + y.impression,
            cash_cost = x.cash_cost + y.cash_cost,
            click = x.click + y.click,
            exp_click = x.exp_click + y.exp_click,
            cvr_num = x.cvr_num + y.cvr_num,
            cvr2_num = x.cvr2_num + y.cvr2_num
          )
      }.coalesce(200)
      .map {
        x =>
          val ctr = x._2.copy(
            exp_click = x._2.exp_click / 1000000
          )
          if (ctr.impression > 0) {
            ctr.copy(
              ctr = ctr.click.toFloat / ctr.impression.toFloat,
              exp_ctr = ctr.exp_click / ctr.impression.toFloat,
              cpm = ctr.cash_cost / ctr.impression.toFloat * (1000 / 100),
              cash_cost = ctr.cash_cost.toInt
            )
          } else {
            ctr.copy(
              cash_cost = ctr.cash_cost.toInt
            )
          }
      }

    clearReportHourData("report_ctr_prediction_hourly", "%s %s:00:00".format(date, hour), "0")
    ctx.createDataFrame(ctrCvrData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_ctr_prediction_hourly", mariadbProp)
    println("ctr", ctrCvrData.count())

    /*
    val cvrlog = ctx.sql(
      s"""
         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" and hour = "%s"
        """.stripMargin.format(date, hour))
      .rdd
      .map {
        x =>
          (x.getAs[String]("searchid"), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = Utils.cvrPositiveV(x._2, "v2")
          (x._1, convert)
      }

    val cvrData = unionLog.filter(_.getAs[Int]("isclick") > 0)
      .map(x => (x.getAs[String]("searchid"), x))
      .leftOuterJoin(cvrlog)
      .map {
        x =>
          val u = x._2._1
          var isload = 0
          var iscvr = 0
          if (x._2._2.isDefined) {
            isload = 1
            iscvr = x._2._2.get
          }

          var exptag = u.getAs[String]("exptags").split(",")
            .find(_.startsWith("cvrmodel"))
            .getOrElse("none")
            .replaceFirst("cvrmodel=", "")

          var cvrthres = u.getAs[Int]("cvr_threshold")

          if (cvrthres <= 0) {
            exptag = "none"
            cvrthres = 0
          } else if (cvrthres <= 10000) {
            cvrthres = 1
          } else if (cvrthres <= 40000) {
            cvrthres = 2
          } else if (cvrthres <= 80000) {
            cvrthres = 3
          } else {
            cvrthres = 4
          }

          val mediaid = u.getAs[String]("media_appsid").toInt
          val adslotid = u.getAs[String]("adslotid").toInt
          val slottype = u.getAs[Int]("adslot_type")
          val adclass = u.getAs[Int]("adclass")
          val expcvr = u.getAs[Int]("exp_cvr").toDouble / 1e6
          var isclick = u.getAs[Int]("isclick")
          var spam_click = u.getAs[Int]("spam_click")
          var antispam_score = u.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = u.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val cost = realCost

          val k = (mediaid, adslotid, adclass, exptag, cvrthres)
          (k, (iscvr, expcvr, isload, 1, cost, slottype))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6)
      }
      .filter(_._2._3 > 0)
      .map {
        x =>
          val k = x._1
          val v = x._2
          val d = "%s %s:00:00".format(date, hour)
          val cvr = v._1.toDouble / v._4.toDouble
          val ecvr = v._2 / v._4.toDouble
          val load = v._3.toDouble / v._4.toDouble

          (k._1, k._2, v._6, k._3, k._4, k._5,
            v._5, v._1, v._2, v._3, v._4, cvr, ecvr, load, d)
      }
      .toDF("media_id", "adslot_id", "adslot_type", "adclass", "exp_tag", "threshold",
        "cash_cost", "cvr_num", "exp_cvr_num", "load_num", "click_num", "cvr", "exp_cvr", "load", "date")

    clearReportHourData("report_cvr_prediction_hourly", "%s %s:00:00".format(date, hour), "0")
    cvrData.write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_cvr_prediction_hourly", mariadbProp)
    println("cvr", cvrData.count())
    */

    val userCharge = unionLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          if (realCost >= 10000 || realCost < 0) {
            realCost = 1
          }
          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            //media_id = x.getAs[String]("media_appsid").toInt,
            //adslot_id = x.getAs[String]("adslotid").toInt,
            //unit_id = x.getAs[Int]("unitid"),
            //idea_id = x.getAs[Int]("ideaid"),
            //plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            //request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            //click = isclick + spam_click,
            click = isclick,
            //charged_click = isclick,
            //spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
          )
          ((charge.user_id, charge.adslot_type), charge)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)
      .map {
        x =>
          (x.user_id, x.adslot_type, x.date, x.hour, x.served_request, x.impression, x.click, x.cash_cost)
      }
      .toDF("user_id", "adslot_type", "date", "hour", "served_request", "impression", "click", "cost")


    clearReportHourData("report_user_charge_hourly", date, hour)
    //    val userChargedata = ctx.createDataFrame(userCharge)
    userCharge.write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_user_charge_hourly", mariadbProp)

    println("userCharge", userCharge.count())


    unionLog.unpersist()

    ctx.stop()
    println("GetHourReport_done")
  }


  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt)
      stmt.executeUpdate(sql)

      if (tbl == "report_media_charge_hourly") {
        val conn_amateur = DriverManager.getConnection(
          mariadb_amateur_url,
          mariadb_amateur_prop.getProperty("user"),
          mariadb_amateur_prop.getProperty("password")
        )
        val stmt_amateur = conn_amateur.createStatement()
        stmt_amateur.executeUpdate(sql)
      }

    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  def clearReportHourData2(tbl: String, date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s"
        """.stripMargin.format(tbl, date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  private case class CtrReport(
                                media_id: Int = 0,
                                adslot_id: Int = 0,
                                adslot_type: Int = 0,
                                adclass: Int = 0,
                                idea_id: Int = 0,
                                unit_id: Int = 0,
                                plan_id: Int = 0,
                                user_id: Int = 0,
                                exp_tag: String = "",
                                request: Int = 0,
                                served_request: Int = 0,
                                impression: Int = 0,
                                cash_cost: Float = 0,
                                click: Int = 0,
                                cvr_num: Int = 0,
                                cvr2_num: Int = 0, //新cvr
                                exp_click: Float = 0,
                                ctr: Float = 0,
                                exp_ctr: Float = 0,
                                cpm: Float = 0,
                                date: String = "",
                                hour: Int = 0
                              )

  private case class ReqDspReport(
                                   media_id: Int = 0,
                                   adslot_id: Int = 0,
                                   adslot_type: Int = 0,
                                   dsp_src: Int = 0,
                                   dsp_mediaid: String = "",
                                   dsp_adslotid: String = "",
                                   dsp_adnum: Int = 0,
                                   request: Int = 0,
                                   fill: Int = 0,
                                   shows: Int = 0,
                                   click: Int = 0,
                                   cash_cost: Int = 0,
                                   date: String = ""
                                 ) {

    def sum(r: ReqDspReport): ReqDspReport = {
      copy(
        request = r.request + request,
        fill = r.fill + fill,
        shows = r.shows + shows,
        click = r.click + click,
        cash_cost = r.cash_cost + cash_cost,
        dsp_adnum = r.dsp_adnum + dsp_adnum
      )
    }
  }

}
