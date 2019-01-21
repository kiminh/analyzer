package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession


object OcpcAccPcvr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    // 按小时累积pcvr数据
    accumulatePCVR(date, hour, spark)

  }

  def accumulatePCVR(date: String, hour: String, spark: SparkSession): Unit = {
    import spark.implicits._

    // 按小时抽取数据
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
         |and searchid is not null
       """.stripMargin

    println(sqlRequest)

    val rawTable = spark.sql(sqlRequest)
    rawTable.createOrReplaceTempView("raw_table")

    // 按小时汇总数据
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


    resultDF
      .repartition(2)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_pcvr_history")

  }

}