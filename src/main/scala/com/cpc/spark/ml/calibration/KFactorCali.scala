package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.ml.calibration.HourlyCalibration._
import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


object KFactorCali {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    //激励视频ctr校准

    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val model = args(3)
    val calimodel = args(4)

    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select count(*) as show,sum(isclick)/sum(raw_ctr)*1e6d as k
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002') and isshow = 1
                 | and ctr_model_name in ('$model','$calimodel') and adtype = 15
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND charge_type in (0,1)
       """.stripMargin

//    val sql = s"""
//                 |select count(*) as show,sum(if(b.searchid is null,0,1))*1e6d/sum(exp_ctr) as k
//                 |from
//                 |(select searchid,isclick,exp_ctr
//                 |from dl_cpc.slim_union_log
//                 |  where dt > '2019-05-31' and dt <'2019-06-06' and adslotid = '1029077' and adtype = 11 and isshow > 0
//                 |  and ctr_model_name in ('$model','$calimodel')) a
//                 |left join
//                 |(select * from dl_cpc.cpc_basedata_trace_event
//                 |where day > '2019-05-31' and day <'2019-06-06'
//                 |and trace_op1 in ('REPORT_USER_STAYINWX_MEANTIME','CONVERSION_CLICK')) b
//                 |on (a.searchid = b.searchid)
//       """.stripMargin
    println(s"sql:\n$sql")
    var k = session.sql(sql).first().getAs[Double]("k")
    val show = session.sql(sql).first().getAs[Long]("show")

    if( k < 0.15){
      k = 0.15
    }

    val irModel = IRModel(
      boundaries = Seq(1.0),
      predictions = Seq(k)
    )
    println(s"k is: $k")
    val config = CalibrationConfig(
      name = calimodel,
      ir = Option(irModel)
    )
    val localPath = saveProtoToLocal(calimodel, config)
    saveFlatTextFileForDebug(calimodel, config)
  }
}