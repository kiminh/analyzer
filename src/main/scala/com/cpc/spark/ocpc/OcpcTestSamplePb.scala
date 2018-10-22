

package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}




object OcpcTestSamplePb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    import spark.implicits._

    // calculate time period for historical data
    val end_date = args(0)
    val hour = args(1)
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(end_date)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -3)
    val dt3 = calendar.getTime
    val date3 = sdf.format(dt3)
    calendar.add(Calendar.DATE, -4)
    val dt = calendar.getTime
    val start_date = sdf.format(dt)
    val selectCondition1 = s"`date`='$start_date' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$start_date' and `date`<'$end_date'"
    val selectCondition3 = s"`date`='$end_date' and hour <= '$hour'"

    // read data and set redis configuration
    val filename = "/user/cpc/wangjun/ocpc_demo.txt"
    val dataRDD = spark.sparkContext.textFile(filename)
    val dataDF = dataRDD.map(x=>(x.split(",")(0), x.split(",")(1), x.split(",")(2), x.split(",")(3))).toDF("ideaid", "cpa_given", "date", "hour")

    dataDF.show(10)
    dataDF.createOrReplaceTempView("test_ocpc_demo")

    println("##############")
    println(selectCondition1)
    println("##############")
    println(selectCondition2)
    println("##############")
    println(selectCondition3)


    val sqlRequest =
      s"""
         |SELECT
         |  a.ideaid,
         |  a.cpa_given,
         |  a.date,
         |  a.hour
         |FROM
         |  test_ocpc_demo a
         |LEFT JOIN
         |   (SELECT
         |      ideaid
         |    FROM
         |      test.ocpc_idea_update_time
         |    WHERE
         |      `update_date` < '$date3'
         |    or
         |      (`update_date`='$date3' and `update_hour`<'$hour')) b
         |ON
         |    a.ideaid=b.ideaid
         |WHERE
         |    b.ideaid is not null
       """.stripMargin

    val userDF = spark.sql(sqlRequest)

    userDF.show(10)

  }



}

