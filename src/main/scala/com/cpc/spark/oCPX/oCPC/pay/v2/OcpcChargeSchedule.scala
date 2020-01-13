package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeSchedule {
  def main(args: Array[String]): Unit = {
    /*
    赔付标识分两块：浅层赔付标识和深层赔付标识，基于is_pay_flag和is_deep_pay_flag：
    last_ocpc_charge_time替换成ocpc_charge_time:
    1. date_diff = 8, is_pay_flag = 1, is_deep_pay_flag = 0
    2. date_diff = 8, is_pay_flag = 1, is_deep_pay_flag = 1
    last_ocpc_charge_time替换成null:
    is_pay_flag = 0

    last_deep_ocpc_charge_time替换成deep_ocpc_charge_time：
    1. is_deep_pay_flag = 1, last_deep_ocpc_charge_time为null
    2. is_deep_pay_flag = 1, date_diff = 8
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val dayCnt = args(2).toInt

    // 获取前一天的ocpc_compensate线上数据（备份表），基于ocpc_charge_time和deep_ocpc_charge_time来判断周期开始日期以及分别需要计算深度还是浅层赔付
    val ocpcCompensate = getOcpcCompensateSchedule(date, version, spark)

    // 统计今天的分单元消耗和开始消费时间
    val todayData = getTodayData(date, spark)

    // 关联赔付周期表
    val data = joinSchedule(ocpcCompensate, todayData, date, spark)


    // 更新赔付周期表
    val result = updateSchedule(data, date, dayCnt, spark)

//    result
//      .repartition(1)
//      .write.mode("overwrite").saveAsTable("test.ocpc_compensate_schedule_daily20191216a")

    val resultDF = result
      .select("unitid", "calc_dates", "date_diff", "pay_cnt", "current_ocpc_charge_time", "current_deep_ocpc_charge_time", "ocpc_charge_time", "deep_ocpc_charge_time", "is_pay_flag", "is_deep_pay_flag", "recent_charge_time")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_compensate_schedule_daily")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_compensate_schedule_daily")

  }

//  def updateSchedule(dataRaw: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
//    /*
//    更新schedule表
//    1. 根据是否有fisrt_charge_time，判断是否有历史，记录，若无历史记录，则fisrt_charge_time, final_charge_time, last_ocpc_charge_time均等于ocpc_charge_time。若有历史记录，则跳过
//    2. 根据是否有last_deep_ocpc_charge_time或者deep_ocpc_charge_time判断是否有历史深度转化数据，若无，则is_deep_pay_flag为0，否则为1
//    3. 根据first_charge_time计算pay_cnt（赔付周期）
//    4. 根据last_charge_time计算赔付所需日数和间隔时间
//    5. 间隔时间如果等于8，则向last_ocpc_charge_time和last_deep_ocpc_charge_time中基于ocpc_charge_time和deep_ocpc_charge_time进行更新
//    6. 根据pay_cnt和last_deep_ocpc_charge_time来判断是否需要继续赔付
//     */
//
////    如果date_diff > 8, last_ocpc_charge_time = recent_charge_time, is_deep_pay_flag=1,
//    val data = dataRaw
//      .select("unitid", "current_ocpc_charge_time", "current_deep_ocpc_charge_time", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt")
//      .withColumn("pay_schedule2", udfCheckDate(date, dayCnt)(col("final_charge_time")))
//      .withColumn("calc_dates", col("pay_schedule2").getItem(1))
//      .withColumn("date_diff", col("pay_schedule2").getItem(2))
//      .withColumn("is_pay_flag", when(col("pay_cnt") < 4, 1).otherwise(0))
//      .na.fill(date + " 00:00:00", Seq("ocpc_charge_time"))
//      .withColumn("is_deep_pay_flag", when(col("last_deep_ocpc_charge_time").isNotNull || col("deep_ocpc_charge_time").isNotNull, 1).otherwise(0))
//      .withColumn("deep_ocpc_charge_time", when(col("is_deep_pay_flag") === 1 && col("deep_ocpc_charge_time").isNull, date + " 00:00:00").otherwise(col("deep_ocpc_charge_time")))
//      .withColumn("recent_charge_time", udfCalculateRecentChargeTime(date)(col("calc_dates")))
//
//    data.createOrReplaceTempView("data")
//    /*
//    date_diff > 8 and is_pay_flag = 1, last_ocpc_charge_time替换成recent_charge_time
//    date_diff > 8 and is_deep_pay_flag = 1, last_deep_ocpc_charge_time替换成recent_charge_time
//
//    last_ocpc_charge_time替换成ocpc_charge_time:
//    1. 周期第一天，且浅层赔付未结束(calc_dates = 1)
//
//    last_deep_ocpc_charge_time替换成deep_ocpc_charge_time:
//    1. 需要深度赔付，赔付周期第一天
//    2. 需要深度赔付，非赔付周期第一天，且上个周期无深度赔付记录
//     */
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  unitid,
//         |  ocpc_charge_time,
//         |  deep_ocpc_charge_time,
//         |  first_charge_time,
//         |  final_charge_time,
//         |  recent_charge_time,
//         |  pay_schedule1,
//         |  pay_cnt,
//         |  pay_schedule2,
//         |  calc_dates,
//         |  date_diff,
//         |  is_pay_flag,
//         |  is_deep_pay_flag,
//         |  flag1,
//         |  last_ocpc_charge_time as last_ocpc_charge_time_old,
//         |  last_deep_ocpc_charge_time as last_deep_ocpc_charge_time_old,
//         |  (case when calc_dates = 1 and pay_cnt = 0 then ocpc_charge_time -- 第一个周期第一天，且浅层赔付未结束
//         |        when pay_cnt > 0 then recent_ocpc_charge_time
//         |        else last_ocpc_charge_time
//         |   end) as last_ocpc_charge_time,
//         |   (case when calc_dates = 1 and is_deep_pay_flag = 1 then deep_ocpc_charge_time -- 需要深度赔付，赔付周期第一天
//         |         when calc_dates > 1 and is_deep_pay_flag = 1 and last_deep_ocpc_charge_time is null then deep_ocpc_charge_time -- 需要深度赔付，非赔付周期第一天，且上个周期无深度赔付记录
//         |         else last_deep_ocpc_charge_time
//         |   end) as last_deep_ocpc_charge_time
//         |FROM
//         |  data
//         |""".stripMargin
//    println(sqlRequest)
//    val result = spark
//      .sql(sqlRequest)
//      .withColumn("last_ocpc_charge_time", when(col("last_ocpc_charge_time").isNotNull, udfSetOcpcChargeTime(date)(col("last_ocpc_charge_time"))).otherwise(col("last_ocpc_charge_time")))
//      .withColumn("last_deep_ocpc_charge_time", when(col("last_deep_ocpc_charge_time").isNotNull, udfSetDeepOcpcChargeTime("2019-12-09")(col("last_deep_ocpc_charge_time"))).otherwise(col("last_deep_ocpc_charge_time")))
//
//
//    result
//  }


  def updateSchedule(dataRaw: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
    /*
    更新schedule表
    1.
     */

    // 如果date_diff > 8, last_ocpc_charge_time = recent_charge_time, is_deep_pay_flag=1,
    val data = dataRaw
      .select("unitid", "current_ocpc_charge_time", "current_deep_ocpc_charge_time", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt")
      .na.fill(date + " 00:00:00", Seq("ocpc_charge_time"))
      .withColumn("pay_schedule", udfCheckDate(date, dayCnt)(col("ocpc_charge_time")))
      .withColumn("calc_dates", col("pay_schedule").getItem(1))
      .withColumn("date_diff", col("pay_schedule").getItem(2))
      .withColumn("pay_cnt_old", col("pay_cnt"))
      .withColumn("pay_cnt", when(col("calc_dates") === 1, col("pay_cnt") + 1).otherwise(col("pay_cnt")))
      .withColumn("pay_cnt", when(col("pay_cnt") < 0, 0).otherwise(col("pay_cnt")))
      .withColumn("is_pay_flag", when(col("pay_cnt") < 4, 1).otherwise(0))
      .withColumn("is_deep_pay_flag", when(col("current_deep_ocpc_charge_time").isNotNull || col("deep_ocpc_charge_time").isNotNull, 1).otherwise(0))
      .withColumn("recent_charge_time", udfCalculateRecentChargeTime(date)(col("calc_dates")))

    data.createOrReplaceTempView("data")
    /*
    last_ocpc_charge_time替换成ocpc_charge_time:
    1. 周期第一天，且浅层赔付未结束(calc_dates = 1)

    last_deep_ocpc_charge_time替换成deep_ocpc_charge_time:
    1. 需要深度赔付，赔付周期第一天
    2. 需要深度赔付，非赔付周期第一天，且上个周期无深度赔付记录
     */
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  current_ocpc_charge_time,
         |  current_deep_ocpc_charge_time,
         |  ocpc_charge_time as ocpc_charge_time_old,
         |  deep_ocpc_charge_time as deep_ocpc_charge_time_old,
         |  pay_schedule,
         |  calc_dates,
         |  date_diff,
         |  is_pay_flag,
         |  is_deep_pay_flag,
         |  recent_charge_time,
         |  pay_cnt_old,
         |  pay_cnt,
         |  (case when calc_dates = 1 and pay_cnt = 0 then current_ocpc_charge_time -- 第一个周期第一天，且浅层赔付未结束
         |        when pay_cnt > 0 then recent_charge_time
         |        else ocpc_charge_time
         |   end) as ocpc_charge_time,
         |   (case when is_deep_pay_flag = 1 and deep_ocpc_charge_time is null then current_deep_ocpc_charge_time -- 需要深度赔付，且第一个深度赔付周期
         |         when calc_dates = 1 and deep_ocpc_charge_time is not null then recent_charge_time -- 需要深度赔付，非深度第一个周期
         |         else deep_ocpc_charge_time
         |   end) as deep_ocpc_charge_time
         |FROM
         |  data
         |""".stripMargin
    println(sqlRequest)
    val result = spark
      .sql(sqlRequest)


    result
  }

  def udfSetOcpcChargeTime(date: String) = udf((deepOcpcChargeTime: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val today = dateConverter.parse(date)
    val ocpcChargeDate = dateConverter.parse(deepOcpcChargeTime.split(" ")(0))

    var result = deepOcpcChargeTime
    if (today.getTime() > ocpcChargeDate.getTime()) {
      result = date + " 00:00:00"
    }
    result
  })

  def udfSetDeepOcpcChargeTime(date: String) = udf((deepOcpcChargeTime: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val today = dateConverter.parse(date)
    val ocpcChargeDate = dateConverter.parse(deepOcpcChargeTime.split(" ")(0))

    var result = deepOcpcChargeTime
    if (today.getTime() > ocpcChargeDate.getTime()) {
      result = date + " 00:00:00"
    }
    result
  })


  def udfCalculateRecentChargeTime(date: String) = udf((calcDates: Int) => {
    // 取历史数据
    val dayCnt = calcDates - 1
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayCnt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val result = date1 + " 00:00:00"
    result
  })


  def udfCheckDate(date: String, dayCnt: Int) = udf((ocpcChargeTime: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val today = dateConverter.parse(date)
    val ocpcChargeDate = dateConverter.parse(ocpcChargeTime.split(" ")(0))
    val dateDiff = (today.getTime() - ocpcChargeDate.getTime()) / (1000 * 60 * 60 * 24)
    val payCnt = (dateDiff + 1) / dayCnt
    val calcDates = dateDiff % dayCnt + 1

    val result = Array(payCnt, calcDates, dateDiff)
    result
  })

  def joinSchedule(ocpcCompensate: DataFrame, todayData: DataFrame, date: String, spark: SparkSession) = {
    /*
    关联周期表与今天的新数据
    1. 过滤ocpcCompensate表中ocpc_charge_time中的第一条记录，记为表1
    2. 过滤ocpcCompensate表中final_charge_time中最后一条记录，记为表2
    3. 表1与表2外关联，记为表3
    4. 令todayData为表4
    5. 表3与表4外关联，记为表5
     */
    // 令todayData为表4
    val data = todayData
      .withColumn("current_ocpc_charge_time", col("ocpc_charge_time"))
      .withColumn("current_deep_ocpc_charge_time", col("deep_ocpc_charge_time"))
      .select("unitid", "current_ocpc_charge_time", "current_deep_ocpc_charge_time")

    // 表3与表4外关联，记为表5
    val result = ocpcCompensate
      .join(data, Seq("unitid"), "outer")
      .select("unitid", "current_ocpc_charge_time", "current_deep_ocpc_charge_time", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt")
      .na.fill(-1, Seq("pay_cnt"))

    result
  }

//  def joinScheduleV2(ocpcCompensate: DataFrame, todayData: DataFrame, spark: SparkSession) = {
//    /*
//    关联周期表与今天的新数据
//    1. 过滤ocpcCompensate表中ocpc_charge_time中的第一条记录，记为表1
//    2. 过滤ocpcCompensate表中final_charge_time中最后一条记录，记为表2
//    3. 表1与表2外关联，记为表3
//    4. 令todayData为表4
//    5. 表3与表4外关联，记为表5
//     */
//    ocpcCompensate.createOrReplaceTempView("ocpc_compensate")
//
//    // 过滤ocpcCompensate表中ocpc_charge_time中的第一条记录，记为表1
//    // 过滤ocpcCompensate表中final_charge_time中最后一条记录，记为表2
//    val sqlRequest1 =
//      s"""
//         |SELECT
//         |  unitid,
//         |  ocpc_charge_time as first_charge_time,
//         |  ocpc_charge_time as last_ocpc_charge_time,
//         |  deep_ocpc_charge_time as last_deep_ocpc_charge_time,
//         |  final_charge_time,
//         |  row_number() over(partition by unitid order by ocpc_charge_time) as seq1,
//         |  row_number() over(partition by unitid order by final_charge_time desc) as seq2
//         |FROM
//         |  ocpc_compensate
//         |WHERE
//         |  ocpc_charge_time != ' '
//         |""".stripMargin
//    println(sqlRequest1)
//    val data1 = spark
//      .sql(sqlRequest1)
//      .filter(s"seq1 = 1")
//      .select("unitid", "first_charge_time")
//      .distinct()
//
//    val data2 = spark
//      .sql(sqlRequest1)
//      .filter(s"seq2 = 1")
//      .select("unitid", "last_ocpc_charge_time", "last_deep_ocpc_charge_time", "final_charge_time")
//      .distinct()
//
//    // 表1与表2外关联，记为表3
//    val data3 = data1.join(data2, Seq("unitid"), "outer")
//
//    // 令todayData为表4
//    val data4 = todayData
//      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time")
//
//    // 表3与表4外关联，记为表5
//    val data5 = data3
//      .join(data4, Seq("unitid"), "outer")
//      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time", "first_charge_time", "last_ocpc_charge_time", "last_deep_ocpc_charge_time", "final_charge_time")
//
//    data5
//  }

  def getTodayData(date: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adslot_type,
         |  adclass,
         |  conversion_goal,
         |  timestamp,
         |  from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss') as ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  date = '$date'
         |""".stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("industry", udfDeterminePayIndustry()(col("adslot_type"), col("adclass"), col("conversion_goal")))
      .filter(s"industry in ('feedapp', 'elds', 'pay_industry', 'siteform_pay_industry')")
      .filter(s"seq = 1")
      .select("unitid", "timestamp", "ocpc_charge_time")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  adslot_type,
         |  adclass,
         |  conversion_goal,
         |  timestamp as deep_timestamp,
         |  from_unixtime(timestamp,'yyyy-MM-dd HH:mm:ss') as deep_ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  date = '$date'
         |AND
         |  deep_ocpc_step = 2
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("industry", udfDeterminePayIndustry()(col("adslot_type"), col("adclass"), col("conversion_goal")))
      .filter(s"industry in ('feedapp', 'elds', 'pay_industry', 'siteform_pay_industry')")
      .filter(s"seq = 1")
      .select("searchid", "unitid", "deep_timestamp", "deep_ocpc_charge_time")

    val data = data1
      .join(data2, Seq("unitid"), "left_outer")
      .select("unitid", "timestamp", "ocpc_charge_time", "deep_timestamp", "deep_ocpc_charge_time")

    data
  }

  def getOcpcCompensateScheduleTemp(date: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // ocpc赔付备份表
    val sqlRequest1 =
      s"""
         |SELECT
         |    a.unitid,
         |    a.pay_cnt,
         |    b.ocpc_charge_time
         |FROM
         |    (SELECT
         |        unitid,
         |        pay_cnt
         |    FROM
         |        dl_cpc.ocpc_pay_cnt_daily_v2
         |    WHERE
         |        `date` = '$date1'
         |    AND
         |        version = 'ocpc') as a
         |LEFT JOIN
         |    (SELECT
         |        unitid,
         |        ocpc_charge_time,
         |        pay_cnt
         |    FROM
         |        dl_cpc.ocpc_pay_data_daily_v2
         |    WHERE
         |        `date` = '$date1'
         |    AND
         |        version = 'ocpc') as b
         |ON
         |    a.unitid = b.unitid
         |""".stripMargin
    println(sqlRequest1)
    val data = spark
        .sql(sqlRequest1)
    data.createOrReplaceTempView("base_data")

    // 整合ocpc_charge_time
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time,
         |  (case when 1 = 2 then ocpc_charge_time else null end) as deep_ocpc_charge_time,
         |  pay_cnt
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest2)
    val result = spark.sql(sqlRequest2)

    result
  }

  def getOcpcCompensateSchedule(date: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // 整合ocpc_charge_time
    // todo
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time,
         |  deep_ocpc_charge_time,
         |  pay_cnt
         |FROM
         |  dl_cpc.ocpc_compensate_schedule_daily
         |WHERE
         |  date = '$date1'
         |AND
         |  version = 'ocpc'
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)

    result
  }


//  def getOcpcCompensate(date: String, dayCnt: Int, spark: SparkSession) = {
//    // ocpc赔付备份表
//    val sqlRequest1 =
//      s"""
//         |SELECT
//         |  unitid,
//         |  (case when ocpc_charge_time = ' ' then null else ocpc_charge_time end) as ocpc_charge_time,
//         |  (case when deep_ocpc_charge_time = ' ' then null else deep_ocpc_charge_time end) as deep_ocpc_charge_time,
//         |  compensate_key
//         |FROM
//         |  dl_cpc.ocpc_compensate_backup_daily
//         |WHERE
//         |  `date` = '$date'
//         |""".stripMargin
//    println(sqlRequest1)
//    val dataRaw = spark.sql(sqlRequest1)
//    dataRaw.createOrReplaceTempView("raw_data")
//
//    // 整合ocpc_charge_time
//    val sqlRequest2 =
//      s"""
//         |SELECT
//         |  unitid,
//         |  ocpc_charge_time,
//         |  deep_ocpc_charge_time,
//         |  compensate_key,
//         |  (case when ocpc_charge_time is null then deep_ocpc_charge_time
//         |        else ocpc_charge_time
//         |   end) as final_charge_time,
//         |   cast(split(compensate_key, '~')[1] as int) as pay_cnt
//         |FROM
//         |  raw_data
//         |""".stripMargin
//    println(sqlRequest2)
//    val data = spark
//        .sql(sqlRequest2)
//        .filter(s"ocpc_charge_time is not null")
//
//    data
//  }

  def udfDeterminePayIndustry() = udf((adslotType: Int, adclass: Int, conversionGoal: Int) => {
    val adclassString = adclass.toString
    val adclass3 = adclassString.substring(0, 3)
    val adclass2 = adclassString.substring(0, 6)
    val siteformPayAdclass = Array("110111", "113102", "118102", "118105", "118106", "118109", "123101", "123102", "123103", "123104", "123105", "123106", "123107", "123108", "123109", "123110", "123111", "123112", "123113", "123114", "123115", "130102", "130104", "130112", "135101", "135102", "135103")
    var result = "others"
    if (adclass3 == "134" || adclass3 == "107") {
      result = "elds"
    } else if (adclass3 == "100" && adslotType != 7) {
      result = "feedapp"
    } else if (adclass3 == "100" && adslotType == 7) {
      result = "yysc"
    } else if (adclass == 110110100 || adclass == 125100100) {
      result = "wzcp"
    } else if (adclass3 == "103" || adclass3 == "111" || adclass3 == "104") {
      result = "pay_industry"
    } else if (siteformPayAdclass.contains(adclass2) && conversionGoal == 3) { // 【ID1091867】新增行业赔付规则-医护&医美&招商加盟
      result = "siteform_pay_industry"
    } else {
      result = "others"
    }
    result

  })

}
