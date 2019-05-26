package com.cpc.spark.OcpcProtoType.model_novel_v3

import java.text.SimpleDateFormat
import java.util.Calendar
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object OcpcSuggestCPAV3 {
  def main(args: Array[String]): Unit = {
    /*
    author:wy
     */
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val hourInt = args(4).toInt


    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa novel_v3: $date, $hour")
      .enableHiveSupport().getOrCreate()

    //根据dl_cpc.dw_unitid_detail确定unitid的conversionGoal以及cpa
    val cpaData = getBaseData(date, hour, spark, 0.8)

    // 实时查询ocpc标记（从mysql抽取）,如广告主是ocpc广告，取广告主给定cpa
    val ocpcFlag = getOcpcFlag(spark)

    // 数据组装
    val result = assemblyData(cpaData, ocpcFlag, spark, 0.7)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF.show(10)

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_cpagiven_novel_v3_hourly")
//    println("successfully save data into table: test.ocpc_suggest_cpa_recommend_hourly_v2")
  }

  def getBaseData(date: String, hour: String, spark: SparkSession, alpha: Double) = {
    /*
    抽取基础表，只包括前一天趣头条上有记录的unitid和对应adclass
     */
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)
    calendar.add(Calendar.DATE, -2)
    val start_date2 = calendar.getTime
    val date2 = sdf.format(start_date2)
    val selectCondition = s"`date` between '$date2' and '$date1'"
    val selectCondition2 = s"day between '$date2' and '$date1'"

    // 点击数据
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    adclass,
         |    price,
         |    bid,
         |    ocpc_log
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE $selectCondition
         |  AND adslot_type in (1,2,3)
         |  AND adsrc = 1
         |  and isclick = 1
         |  AND (charge_type is null or charge_type = 1)
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    // 转化
    val sqlRequest2 =
      s"""
         |select distinct a.searchid,
         |        a.conversion_target as real_target,
         |        b.conversion_target[0] as unit_target
         |from dl_cpc.dm_conversions_for_model a
         |join dl_cpc.dw_unitid_detail b
         |    on a.unitid=b.unitid
         |    and a.day = b.day
         |    and b.$selectCondition2
         |where a.$selectCondition2
         |and size(a.conversion_target)>0
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)
      .withColumn("iscvr",matchcvr(col("real_target"),col("unit_target")))
      .filter("iscvr = 1")

    val sqlRequest3 =
      s"""
         |select
         |  distinct unitid,
         |  (case when conversion_target[0] = "sdk_app_install" then 1
         |          when conversion_target[0] = "api" then 2
         |          when conversion_target[0] = "site_form" then 3
         |          when conversion_target[0] = "sdk_site_wz" then 4
         |          else 0 end) as conversion_goal
         |from dl_cpc.dw_unitid_detail
         |where $selectCondition2
       """.stripMargin

    val conversionData = spark.sql(sqlRequest3)

    // 数据关联
    val data = ctrData
      .join(conversionData, Seq("unitid"),"inner")
      .filter("conversion_goal>0")
      .join(cvrData, Seq("searchid"), "left_outer")
      .withColumn("new_adclass", (col("adclass")/1000).cast(IntegerType))

    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    adclass,
         |    price,
         |    bid as original_bid,
         |    ocpc_log,
         |    iscvr,
         |    real_target,
         |    new_adclass,
         |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['dynamicbid'] as double)
         |          else cast(bid as double) end) as real_bid,
         |    conversion_goal
         |FROM
         |    base_data
       """.stripMargin
    println(sqlRequest)
    val basedata = spark.sql(sqlRequest)
    basedata.show(10)

    val qttCpa = basedata.groupBy("unitid", "new_adclass")
      .agg(
        sum(col("price")).alias("cost"),
        sum(col("iscvr")).alias("cvrcnt"),
        avg(col("real_bid")).alias("qtt_avgbid"))
      .withColumn("qtt_cpa",col("cost")/col("cvrcnt"))
      .withColumn("maxbid",col("qtt_cpa"))
      .withColumn("alpha", col("qtt_cpa") * 1.0 / col("qtt_avgbid"))
      .filter("qtt_cpa is not null")
    qttCpa.createOrReplaceTempView("qtt_cpa_table")
    qttCpa.show(10)

    val sqlRequest4 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha, $alpha) as alpha_max
         |FROM
         |  qtt_cpa_table
         |WHERE
         |  cvrcnt > 1
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest4)
    val alpha1Data = spark.sql(sqlRequest4)
    val resultDF = qttCpa.join(conversionData, Seq("unitid"),"inner")
        .join(alpha1Data,Seq("new_adclass"),"left")
        .withColumn("cpa_max",col("alpha_max")*col("qtt_avgbid"))
        .withColumn("cpagiven",when(col("qtt_cpa")<col("cpa_max"),col("qtt_cpa")).otherwise(col("cpa_max")))

    resultDF.show(10)
    resultDF
  }

  def matchcvr = udf {
    (real_target: Seq[String],unit_target:String) => {
      if (real_target contains(unit_target))  1 else 0
    }
  }

  def getOcpcFlag(spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status, is_ocpc from adv.unit where is_ocpc = 1 and ideas is not null and ocpc_bid > 0) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .selectExpr("unitid", "is_ocpc", "cast(conversion_goal as int) adv_conversion_goal","ocpc_bid")
      .select("unitid","adv_conversion_goal","ocpc_bid")

    resultDF.show(10)
    resultDF
  }

  def assemblyData(baseData: DataFrame,ocpcFlag: DataFrame,spark: SparkSession, wz_discount: Double) = {
    /*
    assemlby the data together
     */
    val result = baseData
      .join(ocpcFlag, Seq("unitid"), "left_outer")
      .withColumn("cpagiven",when(col("ocpc_bid") isNotNull,col("ocpc_bid")).otherwise(col("cpagiven")))
      .withColumn("cpagiven",when(col("new_adclass")===110110,col("cpagiven")* wz_discount).otherwise(col("cpagiven")))
      .withColumn("conversion_goal",when(col("adv_conversion_goal") isNotNull,col("adv_conversion_goal")).otherwise(col("conversion_goal")))

    result.show(50)
    result
  }
}
