package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversion1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val result = getLabel(date, hour, spark)
    result
        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpcv3_cvr1_data_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr1_hourly")
  }

  def getLabel(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label2 as label
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  where $selectWhere
         |AND
         |  label2=1
         |AND
         |  label_type in (1, 2, 3, 4, 5)
         |GROUP BY searchid, label2
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2).distinct()

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}
