package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object unit_ctr_dif {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("unit ctr dif")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    spark.sql(
      s"""
         |select unitid, sum(isclick) as click,
         |         round(sum(isclick)*100.0 / sum(isshow),3) as ctr_real,
         |         avg(if(raw_ctr>0,raw_ctr,0))/10000 as ctr_pred,
         |         avg(if(raw_ctr>0,raw_ctr,0))/(10000*if(round(sum(isclick)*100.0 / sum(isshow),3)>0,round(sum(isclick)*100.0 / sum(isshow),3),0.000000001)) as ratio,
         |         sum(if(isclick=1,price,0)) as cost,
         |         round(sum(if(isclick=1,price,0))*10.000/sum(isshow),4) as cpm,
         |         count(distinct uid) as uv FROM dl_cpc.slim_union_log
         |         WHERE dt='$tardate'
         |         and media_appsid  in ('80000001', '80000002') and isshow > 0
         |         and adsrc = 1
         |         AND userid > 0
         |         and uid not like '%000000%'
         |         and (charge_type = 1 or charge_type is null) group by unitid
      """.stripMargin).repartition(1).createOrReplaceTempView("unit_ctr")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_unitid_ctr_dif partition (dt='$tardate')
         |select unitid, click, ctr_real, ctr_pred, ratio, cost, cpm, uv FROM unit_ctr
      """.stripMargin)

  }
}
