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
  * refined by fym on 2019/01/16 for integration with new base-data routine.
  */
object GetHourReport {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  var mariadb_amateur_url = ""
  val mariadb_amateur_prop = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: GetHourReport <hive_table> <date:string> <hour:string> <databaseToGo:string> [<if_test:int>]
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val table = args(0)
    val date = args(1)
    val hour = args(2)
    val databaseToGo = args(3)
    val if_test = if (args.length > 4) args(4).toInt else 0

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    mariadb_amateur_url = conf.getString("mariadb.amateur_write.url")
    mariadb_amateur_prop.put("user", conf.getString("mariadb.amateur_write.user"))
    mariadb_amateur_prop.put("password", conf.getString("mariadb.amateur_write.password"))
    mariadb_amateur_prop.put("driver", conf.getString("mariadb.amateur_write.driver"))

    val spark = SparkSession.builder()
      .appName("[cpc] get hour report from %s %s/%s"
        .format(table, date, hour))
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    // fym: modified sql to adapt to new unionlog table structure.
    // note: replace charge_type_cpm_or_cpc on charge_type to remove ambiguity
    val unionLog1 = spark.sql(
      s"""
         |select *,
         |      spam_click,
         |      rank_discount,
         |      cvr_threshold,
         |      adclass,
         |      exp_cvr,
         |      exp_ctr,
         |      if(charge_type=2,"cpm","cpc") as charge_type_cpm_or_cpc,
         |      if(charge_type=2,price/1000,price) as charge_fee
         |from dl_cpc.%s where `day` = "%s" and `hour` = "%s" and isfill = 1 and adslot_id > 0 and adsrc <= 1
       """.stripMargin.format(table, date, hour))
      .rdd
      .cache()

    println("unionlog1", unionLog1.count())

    val unionLog = unionLog1.filter(x => x.getAs[String]("charge_type_cpm_or_cpc") == "cpc")

    // 激励广告数据（只加到charge表）
    /*val motive_data = spark.sql(
      s"""
         |select unitid,
         |       planid,
         |       ideaid,
         |       userid,
         |       isfill,
         |       isshow,
         |       isclick,
         |       price as charge_fee,
         |       media_appsid,
         |       adslot_id,
         |       adslot_type,
         |       "cpc" as charge_type,
         |       day,
         |       hour,
         |       0 as spam_click
         |from dl_cpc.%s
         |where `day`="%s" and `hour`="%s" and adslot_type=7 and ideaid>=0 and adsrc <= 1
        """.stripMargin.format(table, date, hour))
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          val chargeType = x.getAs[String]("charge_type")
          var charge_fee = {
            if (isclick > 0 || chargeType == "cpm") {
              x.getAs[Int]("charge_fee")
            }
            else 0D
          }

          if (charge_fee > 10000 || charge_fee < 0) {
            charge_fee = 0
          }

          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslot_id").toInt,
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
            date = x.getAs[String]("day"),
            hour = x.getAs[String]("hour").toInt
          )
          (charge.key, (charge, charge_fee))
      }.rdd

    println("motive", motive_data.count())



    val chargeData = unionLog1
      .filter(_.getAs[Int]("adslot_type") != 7)
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          val chargeType = x.getAs[String]("charge_type_cpm_or_cpc")
          var charge_fee = {
            if (isclick > 0 || chargeType == "cpm")
              x.getAs[Double]("charge_fee")
            else 0D
          }

          if (charge_fee > 10000 || charge_fee < 0) {
            charge_fee = 0
          }

          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslot_id").toInt,
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
            date = x.getAs[String]("day"),
            hour = x.getAs[String]("hour").toInt
          )
          (charge.key, (charge, charge_fee))
      }
      .union(motive_data)
      .reduceByKey((x, y) => (x._1.sum(y._1), x._2 + y._2), 100)
      .map(x => x._2._1.copy(cash_cost = x._2._2.toInt))


    clearReportHourData("report_media_charge_hourly", date, hour)
    val chargedata = spark.createDataFrame(chargeData).persist
    chargedata.write
      .mode(SaveMode.Append)
      .jdbc(
        mariadbUrl,
        if (if_test == 1) "%s.test_report_media_charge_hourly".format(databaseToGo)
        else "%s.report_media_charge_hourly".format(databaseToGo),
        mariadbProp)

    /*chargedata.write
      .mode(SaveMode.Append)
      .jdbc(mariadb_amateur_url,
        if (if_test == 1) "%s.test_report_media_charge_hourly".format(databaseToGo)
        else "%s.report_media_charge_hourly".format(databaseToGo),
        mariadb_amateur_prop)*/

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
            //adslot_id = x.adslot_id.toInt,
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
            date = x.getAs[String]("day"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_geo_hourly", date, hour)
    spark.createDataFrame(geoData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_media_geo_hourly".format(databaseToGo)
        else "%s.report_media_geo_hourly".format(databaseToGo),
        mariadbProp)
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
            adslot_id = x.getAs[String]("adslot_id").toInt,
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
            date = x.getAs[String]("day"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_os_hourly", date, hour)
    spark.createDataFrame(osData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_media_os_hourly".format(databaseToGo)
        else "%s.report_media_os_hourly".format(databaseToGo),
        mariadbProp)
    println("os", osData.count())*/

    val dsplog = spark.sql(
      s"""
         |select
         |  a.searchid,
         |  a.isclick,
         |  a.isfill,
         |  a.isshow,
         |  a.price,
         |  a.media_appsid,
         |  a.adslot_id,
         |  a.adslot_type,
         |  a.adsrc,
         |  a.dsp_num,
         |  b.src as dsp_src,
         |  b.mediaid as dsp_mediaid,
         |  b.adslotid as dsp_adslot_id,
         |  b.adnum as dsp_adnum
         |from dl_cpc.%s a
         |left join dl_cpc.%s b on a.searchid=b.searchid and b.day="%s" and b.hour="%s"
         |where a.day="%s" and a.hour="%s"
      """
        .format(
          table,
          "cpc_basedata_search_dsp",
          date,
          hour,
          date,
          hour)
        .stripMargin
        .trim)
    println("dsplog", dsplog.count())

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
          val adsrc = x.getAs[Int]("adsrc")
          val dsp_src = x.getAs[Int]("dsp_src")

          Seq(ReqDspReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslot_id").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            request = 1,
            date = "%s %s:00:00".format(date, hour),
            dsp_src = dsp_src,
            dsp_mediaid = x.getAs[String]("dsp_mediaid"),
            dsp_adslot_id = x.getAs[String]("dsp_adslot_id"),
            dsp_adnum = x.getAs[Int]("dsp_adnum"),
            fill = if (adsrc == dsp_src) x.getAs[Int]("isfill") else 0,
            shows = if (adsrc == dsp_src) x.getAs[Int]("isshow") else 0,
            click = if (adsrc == dsp_src) isclick else 0,
            cash_cost = if (adsrc == dsp_src) realCost else 0
          ))

          /*for (i <- 0 until dspnum.toInt) {

            /*val src = extInt.getOrElse("dsp_src_" + i, 0L)
            val mediaid = extString.getOrElse("dsp_mediaid_" + i, "")
            val adslot_id = extString.getOrElse("dsp_adslot_id_" + i, "")
            val adnum = extInt.getOrElse("dsp_adnum_" + i, 0L)*/

            val fill = if (src == adsrc) x.getAs[Int]("isfill") else 0
            val shows = if (src == adsrc) x.getAs[Int]("isshow") else 0
            val dsp_click = if (src == adsrc) isclick else 0
            val dsp_cash = if (src == adsrc) realCost else 0
            rows = rows :+ report.copy(
              dsp_src = src.toInt,
              dsp_mediaid = mediaid,
              dsp_adslot_id = adslot_id,
              dsp_adnum = adnum.toInt,
              fill = fill,
              shows = shows,
              click = dsp_click,
              cash_cost = dsp_cash
            )
          }*/
      }
      .map {
        x =>
          val key = (x.media_id, x.adslot_id, x.dsp_src, x.dsp_mediaid, x.dsp_adslot_id, x.date)
          (key, x)
      }
      .reduceByKey((x, y) => x.sum(y), 20)
      .map(x => x._2)

    clearReportHourData2("report_req_dsp_hourly", date + " " + hour + ":00:00")
    spark.createDataFrame(dspdata)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_req_dsp_hourly".format(databaseToGo)
        else "%s.report_req_dsp_hourly".format(databaseToGo),
        mariadbProp)
    println("dsp", dspdata.count())

    val fillLog = spark.sql(
      s"""
         |select *,
         |      spam_click,
         |      rank_discount,
         |      cvr_threshold,
         |      adclass,
         |      exp_cvr,
         |      exp_ctr
         |      from dl_cpc.%s where `day`="%s" and `hour`="%s" and adslot_id > 0 and adsrc <= 1
         |      and (charge_type=1 or charge_type is null)
           """.stripMargin.format(table, date, hour))
      //      .as[UnionLog]
      .rdd
    println("filllog", fillLog.count())

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
            adslot_id = x.getAs[String]("adslot_id").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("day"),
            hour = x.getAs[String]("hour").toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y), 100)
      .map(_._2)

    clearReportHourData("report_media_fill_hourly", date, hour)
    spark.createDataFrame(fillData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_media_fill_hourly".format(databaseToGo)
        else "%s.report_media_fill_hourly".format(databaseToGo),
        mariadbProp)
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
            adslot_id = u.getAs[String]("adslot_id").toInt,
            adslot_type = u.getAs[Int]("adslot_type"),
            adclass = adclass,
            exp_tag = exptag,
            request = 1,
            served_request = u.getAs[Int]("isfill"),
            impression = u.getAs[Int]("isshow"),
            cash_cost = cost,
            click = isclick,
            exp_click = expctr,
            date = "%s %s:00:00".format(u.getAs[String]("day"), u.getAs[String]("hour"))
          )
          (u.getAs[String]("searchid"), ctr)
      }
    unionLog_tmp.unpersist()

    // get cvr data
    // fym: cpc_userid_test_dim 是按日分区的 有取不到数据的风险

    val calBeforeYesterDay = Calendar.getInstance()
    val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd")

    calBeforeYesterDay.set(Calendar.YEAR, date.split("-")(0).toInt)
    calBeforeYesterDay.set(Calendar.MONTH, date.split("-")(1).toInt - 1)
    calBeforeYesterDay.set(Calendar.DAY_OF_MONTH, date.split("-")(2).toInt)
    calBeforeYesterDay.set(Calendar.HOUR_OF_DAY, 0)
    calBeforeYesterDay.set(Calendar.MINUTE, 0)
    calBeforeYesterDay.set(Calendar.SECOND, 0)
    // minus 2 days to get a sure-to-be partition.
    calBeforeYesterDay.add(Calendar.HOUR_OF_DAY,-48)
    
    val cvrlog = spark.sql(
      //      s"""
      //         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" and hour = "%s"
      //            """.stripMargin.format(date, hour))
      s"""
         |select a.searchid as search_id
         |       ,a.adslot_type
         |       ,a.client_type as client_type
         |       ,a.adclass as adclass
         |       ,a.siteid as siteid
         |       ,a.adsrc
         |       ,a.interaction
         |       ,b.*
         |from (select * from dl_cpc.%s
         |        where `day` = "%s" and `hour` = "%s" ) a
         |    left join (select id from bdm.cpc_userid_test_dim where day="%s") t2
         |         on a.userid = t2.id
         |    left join
         |        (select *
         |            from dl_cpc.cpc_basedata_trace_log
         |            where `day` = "%s" and `hour` = "%s"
         |         ) b
         |    on a.searchid=b.searchid
         |where b.searchid is not null and t2.id is null
        """
          .stripMargin
          .format(
            table,
            date,
            hour,
            partitionPathFormat
              .format(calBeforeYesterDay.getTime())/*date*/,
            date,
            hour))
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

    println("cvrlog", cvrlog.count())

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
    spark.createDataFrame(ctrCvrData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_ctr_prediction_hourly".format(databaseToGo)
        else "%s.report_ctr_prediction_hourly".format(databaseToGo),
        mariadbProp)
    println("ctr", ctrCvrData.count())

    /*
    val cvrlog = spark.sql(
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
          val adslot_id = u.getAs[String]("adslot_id").toInt
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

          val k = (mediaid, adslot_id, adclass, exptag, cvrthres)
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
            //adslot_id = x.getAs[String]("adslot_id").toInt,
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
            date = x.getAs[String]("day"),
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
    //    val userChargedata = spark.createDataFrame(userCharge)
    userCharge.write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl,
        if (if_test == 1) "%s.test_report_user_charge_hourly".format(databaseToGo)
        else "%s.report_user_charge_hourly".format(databaseToGo),
        mariadbProp)

    println("userCharge", userCharge.count())


    unionLog.unpersist()

    spark.stop()
    println("-- successfully generated hourly report --")
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

      /*if (tbl == "report_media_charge_hourly") {
        val conn_amateur = DriverManager.getConnection(
          mariadb_amateur_url,
          mariadb_amateur_prop.getProperty("user"),
          mariadb_amateur_prop.getProperty("password")
        )
        val stmt_amateur = conn_amateur.createStatement()
        stmt_amateur.executeUpdate(sql)
      }*/

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
                                   dsp_adslot_id: String = "",
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
