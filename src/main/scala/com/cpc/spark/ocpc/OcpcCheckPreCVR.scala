package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCheckPreCVR {
  def main(args: Array[String]): Unit = {
    /*
    抽取推荐cpa中的unitid，比较昨天和今天的平均预测cvr差距
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString
    val hour2 = args(2).toString

    // 抽取推荐cpa的unitid
    val data = getSuggestUnitid(date, hour, spark)
    // 根据slim_unionlog计算平均预测cvr
    val result = cmpPreCvr(data, date, hour, hour2, spark)

    result.repartition(1).write.mode("overwrite").saveAsTable("test.ocpc_check_pcvr20190122")
  }

  def cmpPreCvr(unitidList: DataFrame, date: String, hour: String, hour2: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // slim_unionlog数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  exp_cvr,
         |  dt as date
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  (`dt`='$date1' or `dt`='$date')
         |AND `hour` >= '$hour'
         |AND isclick=1
       """.stripMargin
    println(sqlRequest)
    val slimUnionlog = spark.sql(sqlRequest)
    slimUnionlog.createOrReplaceTempView("raw_data")

    // 计算两天的数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  date,
         |  sum(exp_cvr) * 0.0000001 / count(1) as pcvr
         |FROM
         |  raw_data
         |GROUP BY unitid, date
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)

    val data1 = data.filter(s"`date`='$date1'").withColumn("pcvr1", col("pcvr")).select("unitid", "pcvr1")
    val data2 = data.filter(s"`date`='$date'").withColumn("pcvr2", col("pcvr")).select("unitid", "pcvr2")

    // 关联unitid与ideaid，获得k
    val kValue = getUnitidK(date, hour, spark)

    val resultDF = data1
      .join(data2, Seq("unitid"), "outer")
      .select("unitid", "pcvr1", "pcvr2")
      .join(unitidList, Seq("unitid"), "inner")
      .select("unitid", "userid", "industry", "conversion_goal", "pcvr1", "pcvr2", "ocpc_flag", "usertype")
      .join(kValue, Seq("unitid", "conversion_goal"), "left_outer")
      .select("unitid", "userid", "industry", "conversion_goal", "pcvr1", "pcvr2", "ocpc_flag", "usertype", "prev_k", "current_k")

    resultDF

  }

  def getUnitidK(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  ideaid
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  `date` between '$date1'
         |GROUP BY unitid, ideaid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val kValue1 = spark.table("dl_cpc.ocpc_pb_result_table_v7")
      .where(s"`date`='$date1' and `hour`='23'")
      .select("ideaid", "kvalue1", "kvalue2")
      .join(data, Seq("ideaid"), "inner")
      .groupBy("unitid")
      .agg(
        avg(col("kvalue1")).alias("prev_k1"),
        avg(col("kvalue2")).alias("prev_k2")
      )

    val kValue2 = spark
      .table("dl_cpc.ocpc_qtt_prev_pb")
      .select("ideaid", "kvalue1", "kvalue2")
      .join(data, Seq("ideaid"), "inner")
      .groupBy("unitid")
      .agg(
        avg(col("kvalue1")).alias("current_k1"),
        avg(col("kvalue2")).alias("current_k2")
      )

    val result = kValue1
      .join(kValue2, Seq("unitid"), "outer")
      .select("unitid", "prev_k1", "prev_k2", "current_k1", "current_k2")

    val result1 = result
      .select("unitid", "prev_k1", "current_k1")
      .withColumn("prev_k", col("prev_k1"))
      .withColumn("current_k", col("current_k1"))
      .withColumn("conversion_goal", lit(1))
      .select("unitid", "prev_k", "current_k", "conversion_goal")


    val result2 = result
      .select("unitid", "prev_k2", "current_k2")
      .withColumn("prev_k", col("prev_k2"))
      .withColumn("current_k", col("current_k2"))
      .withColumn("conversion_goal", lit(2))
      .select("unitid", "prev_k", "current_k", "conversion_goal")

    val result3 = result
      .select("unitid", "prev_k1", "current_k1")
      .withColumn("prev_k", col("prev_k1"))
      .withColumn("current_k", col("current_k1"))
      .withColumn("conversion_goal", lit(3))
      .select("unitid", "prev_k", "current_k", "conversion_goal")

    val resultDF = result1.union(result2).union(result3)

    resultDF

  }



  def getSuggestUnitid(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  industry,
         |  original_conversion as conversion_goal,
         |  ocpc_flag,
         |  usertype
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date1'
         |AND
         |  `hour`='23'
         |AND
         |  version='qtt_demo'
         |AND
         |  is_recommend=1
         |AND
         |  industry in ('feedapp', 'elds')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).distinct()

    data
  }



}

