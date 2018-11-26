
package com.cpc.spark.ocpcV3.ocpcNovel.data

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.ocpc.utils.OcpcUtils._

object OcpcProcessUnionlog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 待测试
    val resultDF = preprocessUnionlog(date, hour, spark)
    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_ctr_data_hourly")
//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_ctr_data_hourly")
    println("successfully save data into table dl_cpc.ocpcv3_ctr_data_hourly")
  }

  def preprocessUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    price,
         |    userid,
         |    media_appsid,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ext_int['is_api_callback'] as is_api_callback
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.createOrReplaceTempView("raw_table")

    // 展现数、点击数、花费
    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  media_appsid,
         |  SUM(case when isclick=1 then price else 0 end) as cost,
         |  SUM(isshow) as show_cnt,
         |  SUM(isclick) as ctr_cnt
         |FROM
         |  raw_table
         |GROUP BY ideaid, adclass, media_appsid
       """.stripMargin
    println(sqlRequest1)
    val groupData1 = spark.sql(sqlRequest1)

    // 非api回传广告的预测转化率累积
    val sqlRequest2 =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  media_appsid,
         |  SUM(isclick) as noapi_click,
         |  SUM(exp_cvr) as noapi_pcvr_total
         |FROM
         |  raw_table
         |WHERE
         |  is_api_callback!=1
         |GROUP BY ideaid, adclass, media_appsid
       """.stripMargin
    println(sqlRequest2)
    val groupData2 = spark.sql(sqlRequest2)

    // api回传类广告的预测转化率累积
    val sqlRequest3 =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  media_appsid,
         |  SUM(isclick) as api_click,
         |  SUM(exp_cvr) as api_pcvr_total
         |FROM
         |  raw_table
         |WHERE
         |  is_api_callback=1
         |GROUP BY ideaid, adclass, media_appsid
       """.stripMargin
    println(sqlRequest3)
    val groupData3 = spark.sql(sqlRequest3)

    // 关联所有数据
    val data = groupData1
      .join(groupData2, Seq("ideaid", "adclass", "media_appsid"), "left_outer")
      .join(groupData3, Seq("ideaid", "adclass", "media_appsid"), "left_outer")

    // TODO 删除临时表
    data.write.mode("overwrite").saveAsTable("test.ocpcv3_base_data_part1")

    val resultDF = data
      .select("ideaid", "adclass", "media_appsid", "cost", "show_cnt", "ctr_cnt", "noapi_click", "noapi_pcvr_total", "api_click", "api_pcvr_total")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }

}


