package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt

    val result = getLabel(conversionGoal, date, hour, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
    //      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel(conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    var sqlRequest = ""
    var cvrPt = ""
    if (conversionGoal == 1) {
      // cvr1数据
      sqlRequest =
        s"""
           |SELECT
           |  searchid,
           |  label2 as label
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition
           |AND
           |  label2=1
           |AND
           |  label_type in (1, 2, 3, 4, 5)
           |GROUP BY searchid, label2
       """.stripMargin
      cvrPt = "cvr1"
    } else if (conversionGoal == 2) {
      // cvr2数据
      sqlRequest =
        s"""
           |SELECT
           |  searchid,
           |  label
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition
           |AND
           |  label=1
           |GROUP BY searchid, label
       """.stripMargin
      cvrPt = "cvr2"
    } else {
      // cvr3数据
      sqlRequest =
        s"""
           |SELECT
           |  searchid,
           |  1 as label
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
      cvrPt = "cvr3"
    }
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .select("searchid", "label")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("cvr_goal", lit(cvrPt))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}
