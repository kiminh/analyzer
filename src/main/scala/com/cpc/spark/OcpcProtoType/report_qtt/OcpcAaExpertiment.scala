package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap

object OcpcAaExpertiment {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcAdExpertiment").enableHiveSupport().getOrCreate()
    joinBaseIsCvr(date, spark)
    println("base and ml_cvr_feature_v1 joined success")
    convStr2Num(date, spark)
    println("str conv to num success")
    calculateIndexValue(date, spark)
    println("has got index value")
    getPreAdInfo(date, spark)
    println("has got yesterday's ad info")
    getData(date, spark)
    println("has got need data")
  }

  // 将base表和ml_cvr_feature_v1等表关联起来
  def joinBaseIsCvr(date: String, spark: SparkSession): Unit ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	a.`date`,
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
        | a.ocpc_log
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
        |        label=1
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
        |    a.`date` = '$preDate'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1,2,3)
        |and
        |    a.adsrc = 1
        |and
        |    (a.charge_type is null or a.charge_type = 1)
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .withColumn("dt", lit(preDate))
      .withColumn("version", lit("qtt_demo"))
      .repartition(200)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_join_base_iscvr")
  }

  // 将ocpc_log_dict中的字符转化成数字
  def convStr2Num(date: String, spark: SparkSession): Unit ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        | `date`,
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
        | iscvr3
        |from
        |	dl_cpc.ocpc_aa_join_base_iscvr
        |where
        |	`date` = '$preDate'
      """.stripMargin
    val data = spark.sql(sql)
      .withColumn("dt", lit(preDate))
      .withColumn("version", lit("qtt_demo"))
      .repartition(200)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_base_index")
  }

  // 计算acp、acb、cpa等指标值
  def calculateIndexValue(date: String, spark: SparkSession): Unit ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |    `date`,
        |    unitid,
        |    userid,
        |    sum(case when isclick = 1 then cpagiven else 0 end) * 0.01
        |    / sum(case when isclick = 1 and cpagiven is not null then 1 else 0 end) as cpagiven,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr1) as cpareal1,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr2) as cpareal2,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr3) as cpareal3,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.1 / sum(isshow), 3) as cpm,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10 / count(distinct uid), 3) as arpu,
        |    sum(isshow) as show,
        |    sum(isclick) as click,
        |    sum(iscvr1) as cv1,
        |    sum(iscvr2) as cv2,
        |    sum(iscvr3) as cv3,
        |    sum(case when isclick = 1 then pcvr else 0 end) * 1.0
        |    / sum(case when isclick = 1 and pcvr is not null then 1 else 0 end) as pre_cvr,
        |    sum(iscvr1) * 1.0 / sum(isclick) as post_cvr1,
        |    sum(iscvr2) * 1.0 / sum(isclick) as post_cvr2,
        |    sum(iscvr3) * 1.0 / sum(isclick) as post_cvr3,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick) as acp,
        |    sum(case when isclick = 1 and dynamicbid is not null then dynamicbid else bid end) * 0.01
        |    / sum(case when isclick = 1 and dynamicbid is not null then 1 else 0 end) as acb,
        |    sum(case when isclick = 1 then kvalue else 0 end) * 1.0
        |    / sum(case when isclick = 1 and kvalue is not null then 1 else 0 end) as kvalue,
        |    round(sum(case when cpagiven is null then 0 else 1 end) * 1.0 / count(unitid), 3) as ratio
        |from
        |    dl_cpc.ocpc_aa_base_index
        |group by
        |    `date`,
        |    unitid,
        |    userid
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("dt", lit(preDate))
      .withColumn("version", lit("qtt_demo"))
      .repartition(200)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_base_index_value")
  }

  // 从filter表中筛选前一天的广告信息
  def getPreAdInfo(date: String, spark: SparkSession): Unit ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	distinct unitid as unitid,
        |	userid,
        |	ocpc_log_dict['conversiongoal'] as conversion_goal,
        |	`date`
        |from
        |	dl_cpc.ocpc_filter_unionlog
        |where
        |	`date` = '$preDate'
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("dt", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_aa_pre_ad_info")
  }

  // 获得统计结果
  def getData(date: String, spark: SparkSession){
    val startDate = getPreDate(date, 7)
    val endDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	b.`date`,
        | b.unitid,
        | b.userid,
        | a.conversion_goal,
        | b.cpagiven,
        | b.cpareal1,
        | b.cpareal2,
        | b.cpareal3,
        | b.cpm,
        | b.arpu,
        | b.show,
        | b.click,
        | b.cv1,
        | b.cv2,
        | b.cv3,
        | b.pre_cvr,
        | b.post_cvr1,
        | b.post_cvr2,
        | b.post_cvr3,
        | b.acp,
        | b.acb,
        | b.kvalue,
        | b.ratio
        |from
        |	dl_cpc.ocpc_aa_pre_ad_info a
        |left join
        |	(select
        |		`date`,
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
        |		`date` >= '$startDate'
        |	and
        |		`date` <= '$endDate') as b
        |on
        |	a.unitid = b.unitid
        |where
        | a.`date` = '$endDate'
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("dt", lit(endDate))
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
