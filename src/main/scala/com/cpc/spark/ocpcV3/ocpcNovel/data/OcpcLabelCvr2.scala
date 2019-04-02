package com.cpc.spark.ocpcV3.ocpcNovel.data

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcLabelCvr2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = getLabelBak(date, hour, spark)
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpcv3_cvr2_data_hourly")
    println("successfully save data into table: dl_cpc.ocpcv3_cvr2_data_hourly")
  }

  def getLabelBak(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    val sqlRequest2 =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
         |AND
         |  media_appsid in ("80001098","80001292","80000001", "80000002", "80002819")
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2)

    val resultDF = labelData
      .groupBy("ideaid", "unitid", "adclass", "media_appsid")
      .agg(sum(col("label")).alias("cvr2_cnt"))
      .select("ideaid", "unitid", "adclass", "media_appsid", "cvr2_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


    resultDF.show(10)
    resultDF.printSchema()

    resultDF

  }

  def getLabel1(date: String, hour: String, spark: SparkSession) = {
    // 普通api回传
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    media_appsid,
         |    adclass,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_user_api_callback_union_log
         |where $selectCondition
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
       """.stripMargin
    println(sqlRequest1)

    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")

    var selectWhere = s"`date`='$date' and hour = '$hour'"
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct()

    val resultDF = rawData
      .join(labelData, Seq("searchid"))
      .select("searchid", "ideaid", "unitid", "adclass", "media_appsid", "label")
    resultDF.show(10)
    resultDF

  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    // 应用商城
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    media_appsid,
         |    adclass,
         |    isclick,
         |    isshow
         |from dl_cpc.ocpcv3_motivation_unionlog_hourly
         |where $selectCondition
       """.stripMargin
    println(sqlRequest1)

    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")

    var selectWhere = s"`date`='$date' and hour = '$hour'"
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct().select("searchid", "label")

    val resultDF = rawData
      .join(labelData, Seq("searchid"))
      .select("searchid", "ideaid", "unitid", "adclass", "media_appsid", "label")
    resultDF.show(10)
    resultDF

  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    // 表单提交类api回传广告
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -2)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql4(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    unitid,
         |    media_appsid,
         |    adclass,
         |    isclick,
         |    isshow
         |from dl_cpc.cpc_basedata_union_events
         |where $selectCondition
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
       """.stripMargin
    println(sqlRequest1)

    val rawData = spark.sql(sqlRequest1)
    rawData.createOrReplaceTempView("raw_table")

    var selectWhere = s"`date`='$date' and hour = '$hour'"
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  where $selectWhere
         |AND
         |  label=1
       """.stripMargin
    println(sqlRequest2)
    val labelData = spark.sql(sqlRequest2).distinct()

    val resultDF = rawData
      .join(labelData, Seq("searchid"))
      .select("searchid", "ideaid", "unitid", "adclass", "media_appsid", "label")
    resultDF.show(10)
    resultDF
  }

  def getLabel(date: String, hour: String, spark: SparkSession) = {
    val labelData1 = getLabel1(date, hour, spark)
//    val labelData2 = getLabel2(date, hour, spark)
    val labelData3 = getLabel3(date, hour, spark)
    labelData1.write.mode("overwrite").saveAsTable("test.ocpcv3_cvr2_data_hourly_label1")
//    labelData2.write.mode("overwrite").saveAsTable("test.ocpcv3_cvr2_data_hourly_label2")
    labelData3.write.mode("overwrite").saveAsTable("test.ocpcv3_cvr2_data_hourly_label3")

    val labelData = labelData1.union(labelData3)
    labelData.show(10)
    labelData.createOrReplaceTempView("label_data")
    val count = labelData.count()
    println(s"result count is $count")
    val resultDF = labelData
      .filter(s"label=1")
      .select("ideaid", "unitid", "adclass", "media_appsid", "label")
      .groupBy("ideaid", "unitid", "adclass", "media_appsid")
      .agg(sum(col("label")).alias("cvr2_cnt"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
    resultDF.show(10)
    resultDF
  }
}