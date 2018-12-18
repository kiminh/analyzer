package com.cpc.spark.ocpcV3.ocpcQtt.warning

import com.cpc.spark.ocpc.GetOcpcLogFromUnionLog.getUnionlog
import org.apache.spark.sql.SparkSession

object OcpcCheckOcpcUnionlog {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession
      .builder()
      .appName(s"ocpc unionlog check: $date, $hour")
      .enableHiveSupport()
      .getOrCreate()

    if (checkDataCount(date, hour, spark)) {
      getUnionlog(date, hour, spark)
      println(s"fill the missing data: '$date', '$hour'")
    } else {
      println(s"no need to fill the missing data: '$date', '$hour'")
    }

  }

  def checkDataCount(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date' and `hour`='$hour'")

    val cnt = data.count()

    if (cnt == 0) {
      true
    } else {
      false
    }
  }

}