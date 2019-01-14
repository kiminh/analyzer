package com.cpc.spark.ocpc.report

import java.util.Properties
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object OcpcDataDetail {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ocpc-report-writer-hourly").enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val result = exportHourlyReport(date, hour, spark)
//    dl_cpc.ocpc_detail_report_hourly
//    data.write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly")
    val data = result
      .withColumn("hour", lit(hour))
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "date", "hour")

    data
      .repartition(10)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_detail_report_hourly")

    saveDataDetailToReport(result, spark)
  }

  def exportHourlyReport(date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // 读取数据
    val apiData = spark
      .table("dl_cpc.ocpc_check_hourly_report_api")
      .where(s"`date`='$date' and `hour`='$hour'")
      .withColumn("conversion_goal", lit(2))
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))

    val noApiData = spark
      .table("dl_cpc.ocpc_check_hourly_report_noapi")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))
      .filter("ctr_cnt>0")

    // 把两个部分数据连接到一起
    val rawData = apiData.union(noApiData)
//    rawData.show(10)

    // 计算其他相关特征
    val data = rawData
      .withColumn("idea_id", col("ideaid"))
      .withColumn("user_id", col("userid"))
      .withColumn("step2_click_percent", col("step2_percent"))
      .withColumn("is_step2", when(col("step2_percent")===1, 1).otherwise(0))
      .withColumn("cpa_ratio", when(col("cvr_cnt").isNull || col("cvr_cnt") === 0, 0.0).otherwise(col("cpa_given") * 1.0 / col("cpa_real")))
      .withColumn("is_cpa_ok", when(col("cpa_ratio")>=0.8, 1).otherwise(0))
      .withColumn("impression", col("show_cnt"))
      .withColumn("click", col("ctr_cnt"))
      .withColumn("conversion", col("cvr_cnt"))
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("show_cvr", col("conversion") * 1.0 / col("impression"))
      .withColumn("cost", col("price") * col("click"))
      .withColumn("acp", col("price"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
      .withColumn("recent_k", when(col("recent_k").isNull, 0.0).otherwise(col("recent_k")))
      .withColumn("cpa_real", when(col("cpa_real").isNull, 9999999.0).otherwise(col("cpa_real")))

//    data.show(10)
    // TODO 删除临时表
//    data.write.mode("overwrite").saveAsTable("test.test_ocpc_export_hourly_report")

    // 输出结果
    val result = data.select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "date", "hour")
//    result.printSchema()
    result.show(10)

    result
  }

  def saveDataDetailToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_data_detail"
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