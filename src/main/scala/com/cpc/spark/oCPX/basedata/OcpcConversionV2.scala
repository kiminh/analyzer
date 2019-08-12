package com.cpc.spark.oCPX.basedata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cv1 = getLabel1(date, hour, spark)
    val cv2 = getLabel2(date, hour, spark)
    val cv3 = getLabel3(date, hour, spark)
    val cv4 = getLabel4(date, hour, spark)

    val result = cv1.union(cv2).union(cv3)
    result
      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_label_cvr_hourly")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_unit_label_cvr_hourly")
  }

  def getLabel4(date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest1 =
      s"""
         |select
         |    distinct searchid,
         |    1 as label
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where $selectCondition
         |and access_channel="sdk"
         |and a = "sdk_site_wz"
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |select
         |    distinct searchid,
         |    1 as label
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'js_active_copywx')
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)



    val resultDF = data1
      .union(data2)
      .select("searchid", "label")
      .distinct()
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("cvr_goal", lit("cvr4"))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest1 =
      s"""
         |select
         |    searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where $selectCondition
         |and access_channel="site"
         |and a in ('ctsite_form')
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition
         |AND
         |  label=1
         |AND
         |  (adclass like '134%' or adclass like '107%')
         |GROUP BY searchid, label
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid
         |FROM
         |  dl_cpc.site_form_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  ideaid>0
         |AND
         |  searchid is not null
         |GROUP BY searchid
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark.sql(sqlRequest3)

    // js加粉
    val sqlRequest4 =
      s"""
         |select
         |    distinct searchid
         |from
         |     dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target, 'js_active_js_form')
       """.stripMargin
    println(sqlRequest4)
    val data4 = spark.sql(sqlRequest4)

    val resultDF = data1
      .union(data2)
      .union(data3)
      .union(data4)
      .distinct()
      .withColumn("label", lit(1))
      .select("searchid", "label")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("cvr_goal", lit("cvr3"))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  label=1
         |GROUP BY searchid, label
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .select("searchid", "label")
      .distinct()
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("cvr_goal", lit("cvr2"))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel1(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label2 as label
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  label2=1
         |AND
         |  label_type in (1, 2, 3, 4, 5)
         |GROUP BY searchid, label2
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .select("searchid", "label")
        .distinct()
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("cvr_goal", lit("cvr1"))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}
