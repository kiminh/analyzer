package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSqlCondition
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object OcpcSuggestCpa{
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc cpc stage data: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 计算costData和cvrData
    val costData = getCost(date, hour, spark)
    val cvr1Data = getCVR("cvr1", date, hour, spark)
    val cvr2Data = getCVR("cvr2", date, hour, spark)
    val cvr3Data = getCVR("cvr3", date, hour, spark)

    val cpa1 = calculateCPA(costData, cvr1Data, date, hour, spark)
    val cpa2 = calculateCPA(costData, cvr2Data, date, hour, spark)
    val cpa3 = calculateCPA(costData, cvr3Data, date, hour, spark)

    // 读取auc数据表
    // todo 使用dl_cpc表
    val aucData = getAUC(version, date, hour, spark)


    // 读取k值数据
    val kvalue = getPbK(date, hour, spark)


    // 调整字段
    val cpa1Data = cpa1.withColumn("conversion_goal", lit(1))
    val cpa2Data = cpa2.withColumn("conversion_goal", lit(2))
    val cpa3Data = cpa3.withColumn("conversion_goal", lit(3))

    val cpaData = cpa1Data
      .union(cpa2Data)
      .union(cpa3Data)
      .select("unitid", "userid", "adclass", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid")
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))

    val resultDF = cpaData
      .join(aucData, Seq("userid", "conversion_goal"), "left_outer")
      .select("unitid", "userid", "adclass", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc")
      .withColumn("original_conversion", col("conversion_goal"))
      .withColumn("conversion_goal", when(col("conversion_goal") === 3, 1).otherwise(col("conversion_goal")))
      .join(kvalue, Seq("unitid", "conversion_goal"), "left_outer")
      .withColumn("cal_bid", col("cpa") * col("pcvr") * col("kvalue"))
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue")
      .withColumn("is_recommend", when(col("auc").isNotNull && col("auc")>0.65, 1).otherwise(0))
      .withColumn("is_recommend", when(col("conversion_goal")===1 && col("pcoc") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("conversion_goal")===1 && col("pcoc") > 1.5, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    test.ocpc_suggest_cpa_recommend_hourly20190104
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly")
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: test.ocpc_suggest_cpa_recommend_hourly")

  }

  def getAUC(version: String, date: String, hour: String, spark: SparkSession) = {
    val auc1Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily")
      .where(s"`date`='$date' and version='$version' and conversion_goal='1'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(1))

    val auc2Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily")
      .where(s"`date`='$date' and version='$version' and conversion_goal='2'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(2))

    val auc3Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily")
      .where(s"`date`='$date' and version='$version' and conversion_goal='3'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(3))

    val resultDF = auc1Data.union(auc2Data).union(auc3Data)
    resultDF
  }

  def getPbK(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql(date1, hour, date, hour)

    // 关联ideaid与unitid
    val data = spark
      .table("dl_cpc.ocpc_ctr_data_hourly")
      .where(selectCondition)
      .select("ideaid", "unitid")
      .distinct()

    // 获取kvalue
    val kvalue1 = spark
      .table("dl_cpc.ocpc_qtt_prev_pb")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "kvalue1")
      .join(data, Seq("ideaid"), "inner")
      .select("unitid", "kvalue1")
      .groupBy("unitid")
      .agg(avg(col("kvalue1")).alias("kvalue"))
      .select("unitid", "kvalue")
      .withColumn("conversion_goal", lit(1))

    val kvalue2 = spark
      .table("dl_cpc.ocpc_qtt_prev_pb")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "kvalue2")
      .join(data, Seq("ideaid"), "inner")
      .select("unitid", "kvalue2")
      .groupBy("unitid")
      .agg(avg(col("kvalue2")).alias("kvalue"))
      .select("unitid", "kvalue")
      .withColumn("conversion_goal", lit(2))

    val resultDF = kvalue1.union(kvalue2).select("unitid", "kvalue", "conversion_goal")

    resultDF
  }

  def getCost(date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
    val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  show_cnt,
         |  ctr_cnt,
         |  total_price,
         |  total_bid,
         |  total_pcvr
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println("############## getCost function ###############")
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .groupBy("unitid", "userid", "adclass")
      .agg(sum(col("show_cnt")).alias("show"),
        sum(col("ctr_cnt")).alias("click"),
        sum(col("total_price")).alias("cost"),
        sum(col("total_bid")).alias("click_bid_sum"),
        sum(col("total_pcvr")).alias("click_pcvr_sum"))
      .select("unitid", "userid", "adclass", "show", "click", "cost", "click_bid_sum", "click_pcvr_sum")

    resultDF
  }

  def getCVR(cvrType: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
	val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val tableName = "dl_cpc.ocpcv3_" + cvrType + "_data_hourly"
    println(s"table name is: $tableName")
    val resultDF = spark
      .table(tableName)
      .where(selectCondition)
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("unitid", "adclass")
      .agg(sum(col(cvrType + "_cnt")).alias("cvrcnt"))
      .select("unitid", "adclass", "cvrcnt")
      .filter("cvrcnt>30 and cvrcnt is not null")


    resultDF
  }

  def calculateCPA(costData: DataFrame, cvrData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = costData
      .join(cvrData, Seq("unitid", "adclass"), "inner")
      .filter("cvrcnt is not null and cvrcnt>0")
      .withColumn("post_ctr", col("click") * 1.0 / col("show"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acb", col("click_bid_sum") * 1.0 / col("click"))
      .withColumn("jfb", col("cost") * 1.0 / col("click_bid_sum"))
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("pcvr", col("click_pcvr_sum") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cvrcnt") * 1.0 / col("click"))
      .withColumn("cal_bid", col("cost") * 1.0 / col("cvrcnt") * (col("click_pcvr_sum") * 1.0 / col("click")))
      .withColumn("pcoc", col("click_pcvr_sum") * 1.0 / col("cvrcnt"))
      .select("unitid", "userid", "adclass", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "cal_bid", "pcoc")
      .filter("cpa is not null and cpa > 0")

    resultDF
  }

}
