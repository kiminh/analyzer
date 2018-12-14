package com.cpc.spark.ocpc

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}


object OcpcHourlySummary {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession
      .builder()
      .appName(s"OcpcHourlySummary: date=$date, hour=$hour")
      .enableHiveSupport()
      .getOrCreate()

    // 获取数据
    val data = joinData(date, hour, spark)
    val costData = calculateCost(data, date, hour, spark)
    val otherData = calculateData(data, date, hour, spark)


    // 关联数据
    val resultDF = otherData
      .join(costData, Seq("conversion_goal"), "left_outer")
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    // 输出数据到hive表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly")
//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_summary_report_hourly")

    // 输出数据到mysql
//    saveDataSummaryToReport(resultDF, spark)
  }

  def joinData(date: String, hour: String, spark: SparkSession) = {
    // 关联数据得到unitid
    val rawData = spark
      .table("dl_cpc.ocpc_detail_report_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k")

    val ideaidList = spark
      .table("dl_cpc.ocpcv3_ctr_data_hourly")
      .where(s"`date`='$date' and `hour` <= '$hour' and media_appsid in ('80000001', '80000002')")
      .withColumn("idea_id", col("ideaid"))
      .withColumn("unit_id", col("unitid"))
      .select("idea_id", "unit_id")
      .distinct()

    val resultDF = rawData
      .join(ideaidList, Seq("idea_id"), "left_outer")
      .select("user_id", "idea_id", "unit_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k")

    resultDF.show(10)

    resultDF
  }

  def calculateCost(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val data = rawData
      .withColumn("click_cpa_given", col("cpa_given") * col("click"))
    data.createOrReplaceTempView("base_data")

    // 计算step2的cost
    val sqlRequest1 =
      s"""
         |SELECT
         |  unit_id,
         |  conversion_goal,
         |  sum(click_cpa_given) / sum(click) as cpa_given,
         |  sum(cost) as cost,
         |  sum(conversion) as conversion
         |FROM
         |  base_data
         |WHERE
         |  is_step2=1
         |GROUP BY unit_id, conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val unitidData = spark
      .sql(sqlRequest1)
      .withColumn("cost_given", col("cpa_given") * col("conversion") * 1.0 / 0.8)
      .withColumn("high_cpa_cost", col("cost") - col("cost_given"))
      .withColumn("high_cpa_cost", when(col("high_cpa_cost") <= 0, 0.0).otherwise(col("high_cpa_cost")))
    unitidData.createOrReplaceTempView("unitid_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  0 as conversion_goal,
         |  SUM(cost) as step2_cost,
         |  SUM(high_cpa_cost) as step2_cpa_high_cost
         |FROM
         |  unitid_data
       """.stripMargin
    println(sqlRequest2)
    val totalCost = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  conversion_goal,
         |  SUM(cost) as step2_cost,
         |  SUM(high_cpa_cost) as step2_cpa_high_cost
         |FROM
         |  unitid_data
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest3)
    val splitCost = spark.sql(sqlRequest3)

    val resultDF = totalCost.union(splitCost).na.fill(0, Seq("step2_cost", "step2_cpa_high_cost"))

    resultDF
  }

  def calculateData(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_table")

    // 基础统计
    val sqlRequest1 =
      s"""
         |SELECT
         |    conversion_goal,
         |    COUNT(1) as total_adnum,
         |    SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |    SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |    SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |    SUM(impression) as impression,
         |    SUM(click) as click,
         |    SUM(conversion) as conversion,
         |    SUM(cost) as cost
         |FROM
         |    raw_table
         |WHERE
         |    conversion_goal is not null
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val rawData1 = spark.sql(sqlRequest1)
    val sqlRequest2 =
      s"""
         |SELECT
         |    0 as conversion_goal,
         |    COUNT(1) as total_adnum,
         |    SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |    SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |    SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |    SUM(impression) as impression,
         |    SUM(click) as click,
         |    SUM(conversion) as conversion,
         |    SUM(cost) as cost
         |FROM
         |    raw_table
         |WHERE
         |    conversion_goal is not null
       """.stripMargin
    println(sqlRequest2)
    val rawData2 = spark.sql(sqlRequest2)
    val data = rawData2
      .union(rawData1)
      .na.fill(0, Seq("total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "impression", "click", "conversion", "cost"))

    // 计算其他数据
    //    ctr
    //    click_cvr
    //    acp
    val result = data
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("click_cvr", when(col("click")===0, 1).otherwise(col("click_cvr")))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acp", when(col("click")===0, 0).otherwise(col("acp")))

    data.show(10)

    val resultDF = result
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp")

    resultDF
  }

  def saveDataSummaryToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_data_summary_v2"
    mariadb_write_url = conf.getString("mariadb.report2_write.url")
    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    println("#################################")
    println("count:" + data.count())
    println("url: " + conf.getString("mariadb.report2_write.url"))
    println("table name: " + tableName)
//    println("user: " + conf.getString("mariadb.report2_write.user"))
//    println("password: " + conf.getString("mariadb.report2_write.password"))
//    println("driver: " + conf.getString("mariadb.report2_write.driver"))
    data.show(10)

    data
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)

  }

}
