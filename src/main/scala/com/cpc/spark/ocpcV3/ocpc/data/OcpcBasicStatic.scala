package com.cpc.spark.ocpcV3.ocpc.data

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcBasicStatic {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val resultDF = getOcpcBasicStatic(date, hour, spark)
    resultDF.write.mode("overwrite").insertInto("test.ocpc_basic_static")
    println("successfully save data into table test.ocpc_basic_static")
  }

  def getOcpcBasicStatic(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"dt='$date' and hour = '$hour'"
    var sqlRequest =
      s"""
         |select
         |    ideaid,
         |    sum(isshow) as shows,
         |    sum(isclick) as clicks,
         |    sum(price) as charge,
         |    avg(exp_ctr) as pre_ctr,
         |    sum(isclick)/sum(isshow) as post_ctr,
         |    avg(exp_cvr) as pre_cvr
         |from dl_cpc.ocpc_unionlog
         |where $selectWhere
         |group by ideaid
      """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val resultDF = data
      .select("ideaid", "shows", "clicks", "charge", "pre_ctr", "post_ctr", "pre_cvr")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
    resultDF
  }
}

