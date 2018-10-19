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
         |SELECT
         |    a.uid,
         |    a.timestamp,
         |    a.searchid,
         |    a.exp_ctr,
         |    a.exp_cvr,
         |    a.isclick,
         |    a.isshow,
         |    a.ideaid,
         |    a.exptags,
         |    a.price,
         |    a.bid_ocpc.
         |    a.ocpc_log,
         |    b.iscvr
         |FROM
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        exptags,
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        hour
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        `date` = '$date'
         |    and
         |        `hour` = '$hour'
         |    and
         |        media_appsid  in ("80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |    AND ext_int['is_ocpc']=1) a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE
         |            `date` = '$date'
         |        and
         |            `hour` = '$hour'
         |    ) b on a.searchid = b.searchid
       """.stripMargin

    val dataDF = spark.sql(sqlRequest)
      .withColumn("cpa_given", udfOcpcLogExtractCPA()(col("ocpc_log")))
      .select("ideaid", "cpa_given")
      .groupBy("ideaid")
      .agg(max("cpa_given").alias("cpa_given"))

    dataDF.show(10)

    dataDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_table")
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
