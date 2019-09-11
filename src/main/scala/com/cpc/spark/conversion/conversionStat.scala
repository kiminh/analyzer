package com.cpc.spark.conversion

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * @author 作者 :wangjun
  * @version 创建时间：2019-05-23
  * @desc
  */

object conversionStat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("[ocpc-monitor] extract log data")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val dayCnt = args(1).toInt
    val minCV = args(2).toInt
    println("parameters:")
    println(s"date=$date")

    val baseData = getBaseData(date, dayCnt, spark)
    val ideaidBase = baseData
      .filter(s"cv > $minCV")
      .select("userid", "unitid", "ideaid", "conversion_target")
      .repartition(10).cache()
    ideaidBase.show(10)
    val unitBase = getUnitConversionTarget(ideaidBase, spark)

    ideaidBase
      .withColumn("date", lit(date))
//      .write.mode("overwrite").insertInto("dl_cpc.idea_conversion_target_daily")
      .write.mode("overwrite").insertInto("test.idea_conversion_target_daily")


    unitBase
      .withColumn("date", lit(date))
//      .write.mode("overwrite").insertInto("dl_cpc.unit_conversion_target_daily")
      .write.mode("overwrite").insertInto("test.unit_conversion_target_daily")


  }

  def getUnitConversionTarget(data: DataFrame, spark: SparkSession) = {
    /*
    统计出每个单元下所有创意转化目标一致的单元，给出各自转化目标
     */
    val base = data
      .select("userid", "unitid", "conversion_target")
      .distinct()
      .cache()

    val unitidCount = base
      .groupBy("userid", "unitid")
      .agg(
        countDistinct(col("conversion_target")).alias("cnt")
      )
      .select("userid", "unitid", "cnt")
      .filter(s"cnt = 1")
      .cache()

    val result = base
      .join(unitidCount, Seq("userid", "unitid"), "inner")
      .select("userid", "unitid", "conversion_target")
      .repartition(10)
      .cache()

    base.unpersist()
    unitidCount.unpersist()
    result.show(10)
    result
  }


  def getBaseData(date: String, dayCnt: Int, spark: SparkSession) = {
    /*
    按照给定时间周期抽取每个单元下每个创意各自不同转化目标的数量，选取创意级别转化数最多的作为自己的转化目标
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSql(date1, "23", date, "23")


    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  userid,
         |  unitid,
         |  ideaid,
         |  a as conversion_target
         |FROM
         |  dl_cpc.cpc_conversion
         |lateral view explode(conversion_target) b as a
         |WHERE
         |  `day` = '$date'
         |AND
         |  size(conversion_target) > 0
       """.stripMargin
    println(sqlRequest1)
    val baseData = spark
      .sql(sqlRequest1)
      .groupBy("userid", "unitid", "ideaid", "conversion_target")
      .agg(
        countDistinct(col("searchid")).alias("cv")
      )
      .select("userid", "unitid", "ideaid", "conversion_target", "cv")
      .filter(s"conversion_target in ('api', 'sdk_app_install', 'sdk_site_wz', 'site_form')")

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  userid,
         |  unitid,
         |  ideaid,
         |  conversion_target,
         |  cv,
         |  row_number() over(partition by userid, unitid, ideaid order by cv desc) as seq
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest2)
    val data = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")

    data

  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`day` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`day` = '$startDate' and hour > '$startHour') " +
      s"or (`day` = '$endDate' and hour <= '$endHour') " +
      s"or (`day` > '$startDate' and `day` < '$endDate'))"
  }

}
