package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val filename = "/user/cpc/wangjun/cpa_given.txt"

    val dataset = testGenCPAgiven(filename, spark)
    dataset.show(10)
    genCPAratio(dataset, date, hour, spark)
    calculateK(spark)

  }

  def testGenCPAgiven(filename: String, spark: SparkSession) = {
    import spark.implicits._
    val data = spark.sparkContext.textFile(filename)

    val resultRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    resultRDD.foreach(println)

    val resultDF = resultRDD.toDF("ideaid", "cpa_given")
    resultDF
  }


  def genCPAratio(CPAgiven: DataFrame, date: String, hour: String, spark:SparkSession) = {
    // 取历史数据
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

    // 计算cpa_given和cpa_history的比值
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

    // 将结果输出到临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_history_ratio")

  }

//  def testCalculateK(spark:SparkSession): Unit = {
//    import spark.implicits._
//    val filename1="/user/cpc/wangjun/ocpc_k.txt"
//    val data = spark.sparkContext.textFile(filename1)
//
//    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
//    dataRDD.foreach(println)
//
//    val dataDF = dataRDD.toDF("ideaid", "k_value")
//    dataDF.show(10)
//
//    val ratioDF = spark.table("test.ocpc_cpa_given_history_ratio")
//
//    dataDF.createOrReplaceTempView("k_table")
//    ratioDF.createOrReplaceTempView("ratio_table")
//
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  a.ideaid,
//         |  (case when a.ratio>1.0 then b.k_value * 1.2
//         |        when a.ratio<1.0 then b.k_value / 1.2
//         |        else b.k_value end) as k_value
//         |FROM
//         |  ratio_table as a
//         |INNER JOIN
//         |  k_table b
//         |ON
//         |  a.ideaid=b.ideaid
//       """.stripMargin
//
//    println(sqlRequest)
//
//    val resultDF = spark.sql(sqlRequest)
//
//    println("final table of the k-value for ocpc:")
//    resultDF.show(10)
//
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table")
//
//  }


  def calculateK(spark:SparkSession): Unit = {
    import spark.implicits._


    val dataDF = spark.table("test.test_new_pb_ocpc").select("ideaid", "k_value")
    dataDF.show(10)

    val ratioDF = spark.table("test.ocpc_cpa_given_history_ratio")

    dataDF.createOrReplaceTempView("k_table")
    ratioDF.createOrReplaceTempView("ratio_table")

    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  (case when a.ratio>1.0 then b.k_value * 1.2
         |        when a.ratio<1.0 then b.k_value / 1.2
         |        else b.k_value end) as k_value
         |FROM
         |  ratio_table as a
         |INNER JOIN
         |  k_table b
         |ON
         |  a.ideaid=b.ideaid
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)

    println("final table of the k-value for ocpc:")
    resultDF.show(10)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table")

  }

}
