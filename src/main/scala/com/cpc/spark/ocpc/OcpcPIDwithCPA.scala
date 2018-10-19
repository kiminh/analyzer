package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfOcpcLogExtractCPA
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    genCPAgiven(date, hour, spark)

    genCPAhistory(date, hour, spark)

  }

  def genCPAgiven(date: String, hour: String, spark:SparkSession) = {

    val sqlRequest =
      s"""
          SELECT

       """.stripMargin

    val dataDF = spark.sql(sqlRequest)

    dataDF.show(10)

    dataDF
  }

  def genCPAhistory(date: String, hour: String, spark:SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition1 = s"`date`='$date1' and hour >= '$hour'"
    val selectCondition2 = s"`date`='$date' and `hour`<='$hour'"

    // read data and calculate cpa_history
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  SUM(cost)/SUM(cvr_cnt) AS cpa_history
         |FROM
         |  dl_cpc.ocpc_uid_userid_track
         |WHERE
         |  ($selectCondition1)
         |OR
         |  ($selectCondition2)
         |GROUP BY ideaid
       """.stripMargin
    println(sqlRequest)

    val dataDF = spark.sql(sqlRequest)
    dataDF.show(10)


  }

}
