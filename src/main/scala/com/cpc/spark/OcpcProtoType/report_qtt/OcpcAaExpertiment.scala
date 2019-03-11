package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap

object OcpcAaExpertiment {

  def main(args: Array[String]): Unit = {
    var date = args(0).toString
    date += " 00:00:00"
    val spark = SparkSession.builder().appName("AaExperitment").enableHiveSupport().getOrCreate()
    println("date：" + date)
    getYesterdayAdInfo(date, spark)
    println("has got yesterday's ad info")
    getBaseUnionlogData(date, spark)
    println("has joined filter and base")
    createTempTable(date, spark)
    println("has extracted index values")
    getData(date, spark)
    println("finish")
  }

  // 首先从ocpc_filter_unionlog表中筛选出前一天的所有不重复的unitid和conversion_voal
  def getYesterdayAdInfo(date: String, spark: SparkSession): Unit ={
    val getInfoSQL =
      s"""
        |select
        |	distinct unitid as unitid,
        | userid,
        |	ocpc_log_dict['conversiongoal'] as conversion_goal,
        |	`date`
        |from
        |	dl_cpc.ocpc_filter_unionlog
        |where
        |	`date` = from_unixtime(unix_timestamp('$date')-24*60*60, 'yyyy-MM-dd')
      """.stripMargin
    val data = spark.sql(getInfoSQL)
    data
      .withColumn("dt", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(5)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_yesterday_ad_info")
  }

  //与ocpc_base_unionlog进行关联，取出相关数据
  def getBaseUnionlogData(date: String, spark: SparkSession): Unit ={
    val joinSQL =
      s"""
        |select
        | b.`date`,
        |	a.unitid,
        |	a.conversion_goal,
        | b.searchid,
        |	b.isclick,
        |	b.isshow,
        |	b.price,
        | b.uid,
        |	b.ocpc_log
        |from
        |	dl_cpc.ocpc_aa_yesterday_ad_info as a
        |left join
        |	dl_cpc.ocpc_base_unionlog b
        |on
        |	a.unitid = b.unitid
        |where
        |	b.`date` >= from_unixtime(unix_timestamp('$date') - 7*24*60*60, 'yyyy-MM-dd')
        |and
        |	b.`date` <= from_unixtime(unix_timestamp('$date') - 24*60*60, 'yyyy-MM-dd')
      """.stripMargin
    val data = spark.sql(joinSQL)
    data
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .withColumn("dt", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(5)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_filter_join_base")
  }

  // 根据Join的表创建包含所有所需字段的临时表
  def createTempTable(date: String, spark: SparkSession): Unit ={
    val tempSQL =
      s"""
        |select
        | a.`date`,
        |	a.unitid,
        |	a.userid,
        |	a.conversion_goal,
        |	a.searchid,
        |	a.isclick,
        |	a.isshow,
        |	a.price,
        |	a.uid,
        |	cast(a.ocpc_log_dict['cpagiven'] as double) as cpagiven,
        |    cast(a.ocpc_log_dict['kvalue'] as double) as kvalue,
        |    cast(a.ocpc_log_dict['pcvr'] as double) as pcvr,
        |    cast(a.ocpc_log_dict['dynamicbidmax'] as double) as dynamicbidmax,
        |    cast(a.ocpc_log_dict['dynamicbid'] as double) as dynamicbid,
        |    (case when a.conversion_goal = 1 then b.iscvr1
        |    	  when a.conversion_goal = 2 then c.iscvr2
        |    	  else d.cvr3 end) as iscvr
        |from
        |	dl_cpc.ocpc_filter_join_base a
        |left join
        |    (select
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` >= from_unixtime(unix_timestamp('$date') - 7*24*60*60, 'yyyy-MM-dd')
        |    and
        |    	`date` <= from_unixtime(unix_timestamp('$date') - 24*60*60, 'yyyy-MM-dd')
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)) as b
        |on
        |    a.searchid = b.searchid
        |left join
        |    (select
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` >= from_unixtime(unix_timestamp('$date') - 7*24*60*60, 'yyyy-MM-dd')
        |    and
        |    	`date` <= from_unixtime(unix_timestamp('$date') - 24*60*60, 'yyyy-MM-dd')
        |    and
        |        label = 1) as c
        |on
        |    a.searchid = c.searchid
        |left join
        |    (select
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    where
        |        `date` >= from_unixtime(unix_timestamp('$date') - 7*24*60*60, 'yyyy-MM-dd')
        |    and
        |    	`date` <= from_unixtime(unix_timestamp('$date') - 24*60*60, 'yyyy-MM-dd')
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null) as d
        |on
        |    a.searchid = d.searchid
      """.stripMargin
    val data = spark.sql(tempSQL)
    data
      .withColumn("dt", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_expertiment_temp")
  }

  // 统计数据结果
  def getData(date: String, spark: SparkSession): Unit ={
    val getDataSQL =
      s"""
        |SELECT
        |    `date`,
        |    unitid,
        |    userid,
        |    sum(case when isclick = 1 then cpagiven else 0 end) * 0.01 / sum(isclick) as cpagiven,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr) as cpareal,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.1 / sum(isshow), 3) as cpm,
        |    round(sum(case WHEN isclick = 1 then price else 0 end) * 10 / count(distinct uid), 3) as arpu,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(iscvr) as cv,
        |    sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
        |    sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick) as acp,
        |    sum(case when isclick = 1 then dynamicbid else 0 end) * 0.01 / sum(isclick) as acb,
        |    sum(case when isclick = 1 then kvalue else 0 end) * 1.0 / sum(isclick) as kvalue,
        |    round(sum(case when ocpc_log_dict is not null then 1 else 0 end) * 1.0 / count(unitid), 3) as ratio
        |FROM
        |    dl_cpc.ocpc_aa_expertiment_temp
        |GROUP BY
        |    `date`,
        |    unitid,
        |    userid
      """.stripMargin
    val data = spark.sql(getDataSQL)
    data
      .withColumn("dt", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_expertiment_data")
  }

}
