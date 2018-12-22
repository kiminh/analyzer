package com.cpc.spark.ocpc

import com.cpc.spark.ocpc.OcpcSampleToRedis.savePbPack
import org.apache.spark.sql.SparkSession


object OcpcRunOver {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_pb_result_table_v5
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
       """.stripMargin

    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val dataCount = data.count()
    println(s"data count is $dataCount")

    data.write.mode("overwrite").saveAsTable("dl_cpc.new_pb_ocpc_with_pcvr")

    // 保存pb文件
    savePbPack(data)
  }
  
}
