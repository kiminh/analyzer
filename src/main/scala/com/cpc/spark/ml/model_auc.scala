package com.cpc.spark.ml

import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics
/**
  * @author guangdong
  * @date 2019/8/21 15:52
  */
object model_auc {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark = SparkSession.builder()
      .appName(s"model auc day = $day")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val date = day.replace("-", "")
    val sql =
      s"""
         |select ctr_model_name
         |    ,exp_ctr as score
         |    ,isclick as label
         |from dl_cpc.cpc_basedata_union_events
         |where day = '$day'
         |and media_appsid in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
         |and adsrc in (1, 28)
         |and userid > 0
         |and ideaid > 0
         |and isshow = 1
         |and uid not like "%.%"
         |and uid not like "%000000%"
         |and length(uid) in (14, 15, 36)
         |and (charge_type is null or charge_type = 1)
         |and length(ctr_model_name) > 0
         |and ctr_model_name not like '%noctr%'
             """.stripMargin
    print(sql)
    val data = spark.sql(sql).cache()
    val ctr_model_name_list = data.rdd.map(x => x.getAs[String]("ctr_model_name")).distinct().collect()
    data.createOrReplaceTempView("base_event")
    spark.sql(
      s"""
         |select
         |    ctr_model_name,
         |    count(*) as show,
         |    sum(score)/1000000/sum(label) as pcoc from base_event group by ctr_model_name
       """.stripMargin).createOrReplaceTempView("show_pcoc")

    for (ctr_model_name <- ctr_model_name_list) {
      println(ctr_model_name)
      val d = data.filter(s"ctr_model_name = '$ctr_model_name'")
      val auc = CalcMetrics.getAuc(spark, d)
      spark.sql(
        s"""
           |insert into dl_cpc.cpc_model_auc partition (day='$day', type='ctr')
           |select '$ctr_model_name', show, '$auc', pcoc from show_pcoc where ctr_model_name='$ctr_model_name'
         """.stripMargin)
    }
  }
}

