package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcUnitConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cv1 = getLabel1(date, hour, spark)
    val cv2 = getLabel2(date, hour, spark)
    val cv3 = getLabel3(date, hour, spark)

    val result = cv1.union(cv2).union(cv3)
    result
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_unit_label_cvr_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel3(date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest1 =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    planid,
         |    userid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where $selectCondition
         |and access_channel="site"
         |and a in ('ctsite_form', 'site_form')
         |and (adclass like '134%' or adclass like '107%')
         |and adslot_type != 7
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  planid,
         |  userid
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition
         |AND
         |  label=1
         |AND
         |  adslot_type != 7
         |AND
         |  (adclass like '134%' or adclass like '107%')
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val resultDF = data1
      .union(data2)
      .distinct()
      .select("searchid", "unitid", "planid", "userid")
      .withColumn("conversion_goal", lit(3))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel2(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and hour = '$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  planid,
         |  userid
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition
         |AND
         |  label=1
         |AND
         |  adslot_type != 7
         |GROUP BY searchid, unitid, planid, userid
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .select("searchid", "unitid", "planid", "userid")
      .withColumn("conversion_goal", lit(2))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }

  def getLabel1(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and hour = '$hour'"
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    planid,
         |    userid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where $selectCondition
         |and access_channel="sdk"
         |and a = "sdk_app_install"
         |and adslot_type != 7
         |group by searchid, unitid, planid, userid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .select("searchid", "unitid", "planid", "userid")
        .withColumn("conversion_goal", lit(1))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF
  }
}
