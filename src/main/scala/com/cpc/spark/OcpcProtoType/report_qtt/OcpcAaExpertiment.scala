package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame}
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap

object OcpcAaExpertiment {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcAdExpertiment").enableHiveSupport().getOrCreate()
    val dataDF = joinBaseIsCvr(date, spark)
    println("base and ml_cvr_feature_v1 joined success")
    val baseIndexDF =  convStr2Num(date, dataDF, spark)
    println("str conv to num success")
    calculateIndexValue(date, baseIndexDF, spark)
    println("has got index value")
    val preAdInfoDF = getPreAdInfo(date, spark)
    println("has got yesterday's ad info")
    getData(date, preAdInfoDF, spark)
    println("has got need data")
  }

  // 将base表和ml_cvr_feature_v1等表关联起来
  def joinBaseIsCvr(date: String, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	a.unitid,
        |	a.userid,
        |	a.searchid,
        |	a.isclick,
        |	a.isshow,
        |	a.price,
        | a.uid,
        | a.bid,
        |	b.iscvr1,
        |	c.iscvr2,
        |	d.iscvr3,
        | a.ocpc_log,
        | a.`date`
        |from
        |	dl_cpc.ocpc_base_unionlog a
        |left join
        |    (select
        |    	searchid,
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` = '$preDate'
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)
        |	) as b
        |on
        |    a.searchid = b.searchid
        |left join
        |    (select
        |    	searchid,
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` = '$preDate'
        |    and
        |        label = 1
        |    ) as c
        |on
        |    a.searchid = c.searchid
        |left join
        |    (select
        |    	searchid,
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    where
        |        `date` = '$preDate'
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null
        |    ) as d
        |on
        |    a.searchid = d.searchid
        |where
        |    `date` = '$preDate'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1, 2, 3)
        |and
        |    a.adsrc = 1
        |and
        |    (a.charge_type is null or a.charge_type = 1)
      """.stripMargin
    val dataDF = spark.sql(sql).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
    dataDF
  }

  // 将ocpc_log_dict中的字符转化成数字
  def convStr2Num(date: String, dataDF: DataFrame, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    dataDF.createOrReplaceTempView("temp_index")
    val sql =
      s"""
        |select
        |	unitid,
        |	userid,
        |	searchid,
        |	isclick,
        |	isshow,
        |	price,
        |	uid,
        | bid,
        |	cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
        | cast(ocpc_log_dict['kvalue'] as double) as kvalue,
        | cast(ocpc_log_dict['pcvr'] as double) as pcvr,
        | cast(ocpc_log_dict['dynamicbidmax'] as double) as dynamicbidmax,
        | cast(ocpc_log_dict['dynamicbid'] as double) as dynamicbid,
        | cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
        | iscvr1,
        | iscvr2,
        | iscvr3,
        | `date`
        |from
        |	temp_index
      """.stripMargin
    val baseIndexDF = spark.sql(sql)
    baseIndexDF
  }

  // 计算acp、acb、cpa等指标值
  def calculateIndexValue(date: String, baseIndexDF: DataFrame, spark: SparkSession): Unit ={
    val preDate = getPreDate(date, 1)
    baseIndexDF.createOrReplaceTempView("base_index")
    val sql =
      s"""
        |select
        |    `date` as dt,
        |    unitid,
        |    userid,
        |    round(sum(case when isclick = 1 then cpagiven else 0 end) * 0.01
        |    / sum(case when isclick = 1 and cpagiven is not null then 1 else 0 end), 4) as cpagiven,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr1), 4) as cpareal1,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr2), 4) as cpareal2,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr3), 4) as cpareal3,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.1 / sum(isshow), 4) as cpm,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10 / count(distinct uid), 4) as arpu,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(iscvr1) as cv1,
        |    sum(iscvr2) as cv2,
        |    sum(iscvr3) as cv3,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0
        |    / sum(case when isclick = 1 and pcvr is not null then 1 else 0 end), 4) as pre_cvr,
        |    round(sum(iscvr1) * 1.0 / sum(isclick), 4) as post_cvr1,
        |    round(sum(iscvr2) * 1.0 / sum(isclick), 4) as post_cvr2,
        |    round(sum(iscvr3) * 1.0 / sum(isclick), 4) as post_cvr3,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick), 4) as acp,
        |    round(sum(case when isclick = 1 and dynamicbid is not null then dynamicbid
        |             when isclick = 1 and dynamicbid is null then bid
        |             else 0 end) * 0.01 / sum(case when isclick = 1 then 1 else 0 end), 4) as acb,
        |    round(sum(case when isclick = 1 then kvalue else 0 end) * 1.0
        |    / sum(case when isclick = 1 and kvalue is not null then 1 else 0 end), 4) as kvalue,
        |    round(sum(case when cpagiven is null then 0 else 1 end) * 1.0 / count(unitid), 4) as ratio
        |from
        |    base_index
        |group by
        |    `date`,
        |    unitid,
        |    userid
      """.stripMargin
    val compIndexValueDF = spark.sql(sql)
    compIndexValueDF
      .withColumn("date", lit(preDate))
      .withColumn("version", lit("qtt_demo"))
      .repartition(200)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_base_index_value")
  }

  // 从filter表中筛选前一天的广告信息
  def getPreAdInfo(date: String, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	unitid,
        |	userid,
        |	ocpc_log_dict['conversiongoal'] as conversion_goal
        |from
        |	dl_cpc.ocpc_filter_unionlog
        |where
        |	`date` = '$preDate'
        |group by
        |	unitid, userid, ocpc_log_dict['conversiongoal']
      """.stripMargin
    val preAdInfoDF = spark.sql(sql)
    preAdInfoDF
  }

  // 获得统计结果
  def getData(date: String, preAdInfoDF: DataFrame, spark: SparkSession){
    val startDate = getPreDate(date, 7)
    val endDate = getPreDate(date, 1)
    preAdInfoDF.createOrReplaceTempView("pre_ad_info")

    val sql =
      s"""
        |select
        |	b.dt,
        | b.unitid,
        | b.userid,
        | a.conversion_goal,
        | b.cpagiven,
        | (case when a.conversion_goal = 1 then b.cpareal1
        |    	  when a.conversion_goal = 2 then b.cpareal2
        |    	  else b.cpareal3 end) as cpareal,
        |  b.cpm,
        |  b.arpu,
        |  b.show,
        |  b.click,
        | (case when a.conversion_goal = 1 then b.cv1
        |    	  when a.conversion_goal = 2 then b.cv2
        |    	  else b.cv3 end) as cv,
        | b.pre_cvr,
        | (case when a.conversion_goal = 1 then b.post_cvr1
        |    	  when a.conversion_goal = 2 then b.post_cvr2
        |    	  else b.post_cvr3 end) as cvr,
        | b.acp,
        | b.acb,
        | b.kvalue,
        | b.ratio
        |from
        |	pre_ad_info a
        |left join
        |	(select
        |		`dt`,
        |	  unitid,
        |	  userid,
        |	  cpagiven,
        |	  cpareal1,
        |	  cpareal2,
        |	  cpareal3,
        |	  cpm,
        |	  arpu,
        |	  show,
        |	  click,
        |	  cv1,
        |	  cv2,
        |	  cv3,
        |	  pre_cvr,
        |	  post_cvr1,
        |	  post_cvr2,
        |	  post_cvr3,
        |	  acp,
        |	  acb,
        |	  kvalue,
        |	  ratio
        |	from
        |		dl_cpc.ocpc_aa_base_index_value
        |	where
        |   `dt` between '$startDate' and '$endDate'
        | and
        |   `dt` is not null) as b
        |on
        |	a.unitid = b.unitid
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("date", lit(endDate))
      .withColumn("version", lit("qtt_demo"))
      .repartition(200)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_expertiment_data")
  }

  // 获得当前时间的前n天
  def getPreDate(date: String, n: Int): String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val timeStamp = sdf.parse(date).getTime
    val needDate = sdf.format(new Date(timeStamp - n * 24 * 60 * 60 * 1000))
    needDate.toString
  }

}
