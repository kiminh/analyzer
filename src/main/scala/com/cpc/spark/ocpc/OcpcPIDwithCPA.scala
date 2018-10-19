package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfOcpcLogExtractCPA
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, max}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

//    import spark.implicits._

    val date = args(0).toString
    val hour = args(1).toString
    val filename = "/user/cpc/wangjun/cpa_given.txt"

//    val data = spark.sparkContext.textFile(filename)
//
//    val resultRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
//    resultRDD.foreach(println)
//
//    val resultDF = resultRDD.toDF("ideaid", "cpa_given")
//    resultDF.show(10)

//    genCPAgiven(date, hour, spark)
    val dataset = testGenCPAgiven(filename, spark)
    dataset.show(10)
    genCPAhistory(dataset, date, hour, spark)

  }

  def testGenCPAgiven(filename: String, spark: SparkSession) = {
    import spark.implicits._
//    filename=" /user/cpc/wangjun/cpa_given.txt"
    val data = spark.sparkContext.textFile(filename)

    val resultRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    resultRDD.foreach(println)

    val resultDF = resultRDD.toDF("ideaid", "cpa_given")
    resultDF
  }


  def genCPAhistory(CPAgiven: DataFrame, date: String, hour: String, spark:SparkSession) = {
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

    val CPAhistory = spark.sql(sqlRequest)
    CPAhistory.show(10)
    CPAhistory.printSchema()

    CPAgiven.createOrReplaceTempView("cpa_given_table")
    CPAhistory.createOrReplaceTempView("cpa_history_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  a.ideaid,
         |  (a.cpa_given * 1.0 / b.cpa_history) as ratio
         |FROM
         |  cpa_given_table a
         |INNER JOIN
         |  cpa_history_table b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin

    println(sqlRequest2)

    val resultDF = spark.sql(sqlRequest2)
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_history_ratio")


  }

}
