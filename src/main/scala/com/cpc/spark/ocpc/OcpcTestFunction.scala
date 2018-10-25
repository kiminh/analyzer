package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcTestFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    accumulatePCVR(date, hour, spark)

  }


  def readInnocence(spark: SparkSession): DataFrame ={
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_ideaid.txt"
    val data = spark.sparkContext.textFile(filename)

    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    dataRDD.foreach(println)

    dataRDD.toDF("ideaid", "flag").createOrReplaceTempView("innocence_list")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  flag
         |FROM
         |  innocence_list
         |GROUP BY ideaid, flag
       """.stripMargin

    val resultDF = spark.sql(sqlRequest)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_innocence_idea_list")
    resultDF
  }

  def accumulatePCVR(date: String, hour: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ext['adclass'].int_value as adclass,
         |  ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |  '$date' as `date`,
         |  '$hour' as `hour`
         |FROM
         |    dl_cpc.cpc_union_log
         |WHERE
         |    `date` = '$date'
         |and
         |    `hour` = '$hour'
         |and
         |    media_appsid  in ("80000001", "80000002")
         |and
         |    ext['antispam'].int_value = 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |and isclick=1
       """.stripMargin

    println(sqlRequest)

    val rawTable = spark.sql(sqlRequest)
    rawTable.createOrReplaceTempView("raw_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  AVG(exp_cvr) as pcvr,
         |  COUNT(1) as cnt,
         |  SUM(exp_cvr) as total_cvr,
         |  '$date' as date,
         |  '$hour' as hour
         |FROM
         |  raw_table
         |GROUP BY ideaid, adclass
       """.stripMargin

    println(sqlRequest2)

    val resultDF = spark.sql(sqlRequest2)


    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_hpcvr_test_20181025")

    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_pcvr_history")

  }

  def caclulateHPCVR(endDate: String, hour: String, spark: SparkSession) ={
    // calculate time period for historical data
    val threshold = 20
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(endDate)
    val calendar = Calendar.getInstance
    calendar.setTime(date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val startDate = sdf.format(dt)
    val selectCondition1 = s"`date`='$startDate' and hour > '$hour'"
    val selectCondition2 = s"`date`>'$startDate' and `date`<'$endDate'"
    val selectCondition3 = s"`date`='$endDate' and hour <= '$hour'"

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  SUM(total_cvr) * 1.0 / SUM(cnt) as hpcvr
         |FROM
         |  dl_cpc.ocpc_pcvr_history
         |WHERE ($selectCondition1) OR
         |      ($selectCondition2) OR
         |      ($selectCondition3)
         |GROUP BY ideaid, adclass
       """.stripMargin
    println(sqlRequest)

    val rawTable = spark.sql(sqlRequest)

    rawTable.write.mode("overwrite").saveAsTable("test.ocpc_hpcvr_test_20181025_total")


  }



}
