package com.cpc.spark.oCPX.oCPC.pay

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeSchedule {
  def main(args: Array[String]): Unit = {
    /*
    ocpc周期控制模块
    结果表包括以下字段：unitid, pay_cnt, pay_date, flag

    pay_cnt: 已经完成赔付的次数
    pay_date: 当前赔付周期的起始日期
    flag: 当前周期是否需要计算赔付(基于pay_cnt判断)

    pay_cnt的更新：
    根据pay_date，date，dayCnt判断该赔付周期是否结束，如果当前周期结束，pay_cnt++

    pay_date的更新：
    更新逻辑同上
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val dayCnt = args(2).toInt

    // 兼容逻辑：兼容老版本的逻辑
    // 抽取基本数据
    val scheduleData = getPaySchedule(date, version, spark)
//    scheduleData
//      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010a")

    // 更新pay_cnt，pay_date
    val updateScheduleData = updatePaySchedule(date, dayCnt, scheduleData, spark)
//    updateScheduleData
//      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010b")

    updateScheduleData
      .select("unitid", "pay_cnt", "pay_date", "flag", "update_flag", "prev_pay_cnt", "prev_pay_date")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pay_cnt_daily_v2")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pay_cnt_daily_v2")



  }


  def updatePaySchedule(date: String, dayCnt: Int, baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt,
         |  pay_date,
         |  cast(date_add(pay_date, $dayCnt) as string) as end_date,
         |  '$date' as cur_date
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_data")
//    rawData
//      .repartition(10)
//      .write.mode("overwrite").saveAsTable("test.ocpc_pay_data20191010c")


    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt,
         |  pay_date,
         |  end_date,
         |  cur_date,
         |  (case when end_date <= cur_date and pay_cnt < 4 then 1 else 0 end) as update_flag
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest2)

    val data = spark
      .sql(sqlRequest2)
//      .withColumn("flag", when(col("pay_cnt") < 4, 1).otherwise(0))
//      .withColumn("flag", when(col("pay_date") > date, 0).otherwise(col("flag")))
      .withColumn("prev_pay_cnt", col("pay_cnt"))
      .withColumn("prev_pay_date", col("pay_date"))
      .withColumn("pay_cnt", when(col("update_flag") === 1, col("pay_cnt") + 1).otherwise(col("pay_cnt")))
      .withColumn("pay_date", when(col("update_flag") === 1, lit(date)).otherwise(col("pay_date")))
      .withColumn("flag", when(col("pay_cnt") < 4, 1).otherwise(0))

    data
  }

  def getPaySchedule(date: String, version: String, spark: SparkSession) = {
    // 获取老版的单元周期数据
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val prevData = spark
      .table("dl_cpc.ocpc_pay_cnt_daily_v2")
      .where(s"`date` = '$date1' and version = '$version'")
      .select("unitid", "pay_cnt", "pay_date")
      .distinct()

//    val prevData = spark
//      .table("dl_cpc.ocpc_pay_cnt_daily")
//      .where(s"`date` = '2019-10-14'")

    // 抽取媒体id，获取当天的数据
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |  media_appsid,
         |  adslot_type,
         |  adclass,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  `date` = '$date'
         |AND
         |  $mediaSelection
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val newDataRaw = spark
      .sql(sqlRequest)
      .filter(s"is_hidden = 0")
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .filter(s"media in ('qtt', 'hottopic', 'novel')")
      .withColumn("industry", udfDeterminePayIndustry()(col("adslot_type"), col("adclass"), col("conversion_goal")))
      .distinct()

//    newDataRaw
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_pay_data20191021a")

    val newData = newDataRaw
      .filter(s"industry in ('feedapp', 'elds', 'pay_industry', 'siteform_pay_industry')")
      .select("unitid")
      .distinct()

//    newData
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_pay_data20191021b")


    val data = prevData
      .join(newData, Seq("unitid"), "outer")
      .select("unitid", "pay_cnt", "pay_date")
      .na.fill(0, Seq("pay_cnt"))
      .na.fill(date, Seq("pay_date"))
//      .filter(s"pay_date <= '$date'")

    data.printSchema()
    data
  }

  def udfDeterminePayIndustry() = udf((adslotType: Int, adclass: Int, conversionGoal: Int) => {
    val adclassString = adclass.toString
    val adclass3 = adclassString.substring(0, 3)
    val siteformPayAdclass = Array(130112100, 123100100, 130104100, 118106100, 118109100, 110111100, 118102100, 118105100, 113102100, 130102100, 135101100, 135102100, 135103100)
    var result = "others"
    if (adclass3 == "134" || adclass3 == "107") {
      result = "elds"
    } else if (adclass3 == "100" && adslotType != 7) {
      result = "feedapp"
    } else if (adclass3 == "100" && adslotType == 7) {
      result = "yysc"
    } else if (adclass == 110110100 || adclass == 125100100) {
      result = "wzcp"
    } else if (adclass == 103100100 || adclass == 111100100 || adclass == 104100100) {
      result = "pay_industry"
    } else if (siteformPayAdclass.contains(adclass) && conversionGoal == 3) { // 【ID1091867】新增行业赔付规则-医护&医美&招商加盟
      result = "siteform_pay_industry"
    } else {
      result = "others"
    }
    result

  })


}
