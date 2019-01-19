package com.cpc.spark.ocpcV3.HotTopicOcpc.HotTopic_report

import java.util.Properties
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object OcpcHotTopicHourlyReport {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val rawData = getHourlyReport(date, hour, spark)
    val result = calculateData(rawData, date, hour, spark)
    val tableName = "dl_cpc.ocpcv3_hottopic_report_detail_hourly"
    result
      .repartition(10).write.mode("overwrite").insertInto(tableName)
    //    result.write.mode("overwrite").saveAsTable(tableName)
    println(s"successfully save table into $tableName")
//    saveDataToReport(result, spark)
  }

  def getHourlyReport(date: String, hour: String, spark: SparkSession) = {
    // 获得基础数据
    val selectCondition = s"`date`='$date' and `hour`<='$hour'"
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    price,
         |    ocpc_log_dict['kvalue'] as kvalue,
         |    ocpc_log_dict['cpahistory'] as cpahistory,
         |    ocpc_log_dict['cpagiven'] as cpagiven,
         |    ocpc_log_dict['dynamicbid'] as bid,
         |    ocpc_log_dict['ocpcstep'] as ocpc_step,
         |    ocpc_log_dict['conversiongoal'] as conversion_goal,
         |    isshow,
         |    isclick,
         |    hour
         |FROM
         |    dl_cpc.ocpc_union_log_hourly
         |WHERE
         |    $selectCondition
           and media_appsid = "80002819"
         |
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    label2 as iscvr1
         |FROM
         |    dl_cpc.ml_cvr_feature_v1
         |WHERE
         |    $selectCondition
         |AND
         |    label2=1
         |AND
         |    label_type!=12
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark.sql(sqlRequest2).distinct()

    val sqlRequest3 =
      s"""
         |SELECT
         |    searchid,
         |    label as iscvr2
         |FROM
         |    dl_cpc.ml_cvr_feature_v2
         |WHERE
         |    $selectCondition
         |AND
         |    label=1
       """.stripMargin
    println(sqlRequest3)
    val cvr2Data = spark.sql(sqlRequest3).distinct()

    // 关联数据
    val data = rawData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
    data.createOrReplaceTempView("data_table")

    // 计算指标
    val sqlRequest4 =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    sum(case when ocpc_step==2 then isclick else 0 end) * 1.0 / sum(isclick) as step2_percent,
         |    SUM(case when isclick==1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |    SUM(case when isclick==1 then price else 0 end) as cost,
         |    SUM(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr1) as cvr1_cnt,
         |    SUM(iscvr2) as cvr2_cnt,
         |    sum(case when isclick=1 then kvalue else 0 end) * 1.0 / sum(isclick) as avg_k,
         |    SUM(case when isclick=1 and `hour`='$hour' then kvalue else 0 end) * 1.0 / sum(case when `hour`='$hour' then isclick else 0 end) as recent_k
         |FROM
         |    data_table
         |GROUP BY unitid, userid, conversion_goal
       """.stripMargin
    println(sqlRequest4)
    val result = spark
      .sql(sqlRequest4)
      .withColumn("step2_percent", when(col("step2_percent").isNull, 0).otherwise(col("step2_percent")))
      .withColumn("cost", when(col("cost").isNull, 0).otherwise(col("cost")))
      .withColumn("ctr_cnt", when(col("ctr_cnt").isNull, 0).otherwise(col("ctr_cnt")))
      .withColumn("cvr1_cnt", when(col("cvr1_cnt").isNull, 0).otherwise(col("cvr1_cnt")))
      .withColumn("cvr2_cnt", when(col("cvr2_cnt").isNull, 0).otherwise(col("cvr2_cnt")))
      .withColumn("avg_k", when(col("avg_k").isNull, 0).otherwise(col("avg_k")))
      .withColumn("recent_k", when(col("recent_k").isNull, 0).otherwise(col("recent_k")))
      .withColumn("cvr_cnt", when(col("conversion_goal")===1, col("cvr1_cnt")).otherwise(col("cvr2_cnt")))
      .withColumn("cpa_real", col("cost") * 1.0 / col("cvr_cnt"))
      .withColumn("cpa_real", when(col("cvr_cnt")===0, 9999999).otherwise(col("cpa_real")))


    val resultDF = result
      .select("unitid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "avg_k", "recent_k", "cost")
      .filter(s"conversion_goal is not null and cpa_given is not null")

    resultDF
  }

  def calculateData(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val result = data
      .withColumn("is_step2", when(col("step2_percent")===1, 1).otherwise(0))
      .withColumn("cpa_ratio", col("cpa_given") * 1.0 / col("cpa_real"))
      .withColumn("cpa_ratio", when(col("cpa_real")===0, 9999999).otherwise(col("cpa_ratio")))
      .withColumn("is_cpa_ok", when(col("cpa_ratio")>=0.5, 1).otherwise(0))
      .withColumn("impression", col("show_cnt"))
      .withColumn("click", col("ctr_cnt"))
      .withColumn("conversion", col("cvr_cnt"))
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("click_cvr", when(col("click")===0, 1).otherwise(col("click_cvr")))
      .withColumn("show_cvr", col("conversion") * 1.0 / col("impression"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acp", when(col("click")===0, 0).otherwise(col("acp")))
      .withColumn("step2_click_percent", col("step2_percent"))

    val resultDF = result
      .select("unitid", "userid", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def saveDataToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_hottopic_data_detail"
    mariadb_write_url = conf.getString("mariadb.report2_write.url")
    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    println("#################################")
    println("count:" + data.count())
    println("url: " +      conf.getString("mariadb.report2_write.url"))
    println("table name: " + tableName)
    println("user: " +     conf.getString("mariadb.report2_write.user"))
    println("password: " + conf.getString("mariadb.report2_write.password"))
    println("driver: " +   conf.getString("mariadb.report2_write.driver"))
    data.show(10)

    data
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)

  }

}
