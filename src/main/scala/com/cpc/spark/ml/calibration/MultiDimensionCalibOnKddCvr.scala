package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.cpc.spark.OcpcProtoType.model_novel_v3.OcpcSuggestCPAV3.matchcvr
import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql4
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel, PostCalibrations}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import com.cpc.spark.ml.calibration.HourlyCalibration.{saveProtoToLocal,saveFlatTextFileForDebug}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.mllib.regression.IsotonicRegression
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttCvr.LogToPb
/**
  * @author dongjinbao
  * @date 2019/9/16 11:09
  */
object MultiDimensionCalibOnKddCvr {
    val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
    val destDir = "/home/work/mlcpp/calibration/"
    val newDestDir = "/home/cpc/model_server/calibration/"
    val MAX_BIN_COUNT = 10
    val MIN_BIN_SIZE = 100000

    def main(args: Array[String]): Unit = {
        Logger.getRootLogger.setLevel(Level.WARN)

        // new calibration
        val endDate = args(0)
        val endHour = args(1)
        val hourRange = args(2).toInt
        val media = args(3)
        val model = args(4)
        val calimodel = args(5)
        val k = args(6)
        val conf = ConfigFactory.load("ocpc")
        val conf_key = "medias." + media + ".media_selection"
        val mediaSelection = conf.getString(conf_key)

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
        val session = Utils.buildSparkSession("Kdd hourlyCalibration")
//        val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)
//        val selectCondition2 = getTimeRangeSql4(startDate, startHour, endDate, endHour)
//        val selectCondition3 = s"day between '$startDate' and '$endDate'"

        val selectCondition = s"((day = '$startDate' and hour > '$startHour') or (day = '$endDate' and hour <= '$endHour') or (day > '$startDate' and day < '$endDate'))"
        val selectCondition2 = s"((`date` = '$startDate' and hour > '$startHour') or (`date` = '$endDate' and hour <= '$endHour') or (`date` > '$startDate' and `date` < '$endDate'))"
        // get union log
        val sql =
            s"""
               |select searchid
               |    ,cast(raw_cvr as bigint) as ectr
               |    ,substring(adclass, 1, 6) as adclass
               |    ,cvr_model_name as model
               |    ,adslot_id as adslotid
               |    ,ideaid
               |    ,case when user_show_ad_num = 0 then '0'
               |          when user_show_ad_num = 1 then '1'
               |          when user_show_ad_num = 2 then '2'
               |          when user_show_ad_num in (3, 4) then '4'
               |          when user_show_ad_num in (5, 6, 7) then '7'
               |          else '8'
               |        end as user_show_ad_num
               |    ,isbuy as isclick
               |from
               |(
               |    select
               |        A.*
               |        ,case when isclick = 1 and conversion_goal = 1 and B.cv_types like '%cvr1%' then 1
               |            when isclick = 1 and conversion_goal = 2 and B.cv_types like '%cvr2%' then 1
               |            when isclick = 1 and conversion_goal = 3 and B.cv_types like '%cvr3%' then 1
               |            when isclick = 1 and conversion_goal = 4 and B.cv_types like '%cvr4%' then 1
               |            when isclick = 1 and conversion_goal = 0 and is_api_callback = 1 and B.cv_types like '%cvr2%' then 1
               |            when isclick = 1 and conversion_goal = 0 and is_api_callback = 0 and (adclass like '11011%' or adclass like '125%') and B.cv_types like '%cvr4%' then 1
               |            when isclick = 1 and conversion_goal = 0 and is_api_callback = 0 and adclass not like '11011%' and adclass not like '125%' and B.cv_types like '%cvr%' then 1
               |        else 0 end as isbuy
               |    from
               |    (
               |        select searchid
               |            ,`timestamp` as ts
               |            ,media_appsid
               |            ,uid
               |            ,ideaid
               |            ,adslot_type
               |            ,adslot_id
               |            ,adtype
               |            ,isshow
               |            ,isclick
               |            ,conversion_goal
               |            ,is_api_callback
               |            ,adclass
               |            ,raw_cvr
               |            ,cvr_model_name
               |            ,user_show_ad_num
               |        from
               |            dl_cpc.cpc_basedata_union_events
               |        where
               |            $selectCondition
               |            and media_appsid in ("80002819", "80004944")
               |            and adsrc in (1, 28)
               |            and userid > 0
               |            and ideaid > 0
               |            and isclick = 1
               |            and cvr_model_name in ('$calimodel','$model')
               |            and (charge_type is null or charge_type = 1)
               |    ) A
               |    left outer join
               |    (
               |        select
               |            searchid
               |            ,concat_ws(',', collect_set(cvr_goal)) as cv_types
               |        from dl_cpc.ocpc_label_cvr_hourly
               |        where $selectCondition2
               |        and label=1
               |        group by searchid
               |    ) B
               |    on A.searchid = B.searchid
               |) final
               |""".stripMargin
        println(sql)
        val log = session.sql(sql).cache()
        log.show(10)
        LogToPb(log, session, calimodel)
        val irModel = IRModel(
            boundaries = Seq(1.0),
            predictions = Seq(k.toDouble)
        )
        println(s"k is: $k")
        val caliconfig = CalibrationConfig(
            name = calimodel,
            ir = Option(irModel)
        )
        val localPath = saveProtoToLocal(calimodel, caliconfig)
        saveFlatTextFileForDebug(calimodel, caliconfig)
    }
}
