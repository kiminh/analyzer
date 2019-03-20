package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val cvrPt = args(2).toString


    println("parameters:")
    println(s"date=$date, hour=$hour, cvrPt=$cvrPt")
    val result = getLabel(cvrPt, date, hour, spark)
    result
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel(cvrPt: String, date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest1 =
      s"""
         |select
         |    searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where `date`='2019-03-20' and hour='12'
         |and access_channel="site"
         |and a in ('site_form','ctsite_form')
         |and (adclass like '134%' or adclass like '107%')
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
    val data2 = spark.sql(sqlRequest2)

    val resultDF = data1
      .union(data2)
      .distinct()
      .withColumn("label", lit(1))
      .select("searchid", "label")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("cvr_goal", lit(cvrPt))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}
