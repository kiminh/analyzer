package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.cpc.spark.OcpcProtoType.report_qtt.OcpcHourlyAucReport

object OcpcAaExpertiment {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcAdExpertiment").enableHiveSupport().getOrCreate()
    val dataDF = joinBaseIsCvr(date, spark)
    println("base and ml_cvr_feature_v1 joined success")
    val baseIndexDF =  convStr2Num(dataDF, spark)
    println("str conv to num success")
    val isHiddenEtcIndexDF = getIsHiddenEtcIndex(date, spark)
    println("has got is hidden etc index")
    val suggestCpaDF = getSuggestCpa(date, spark)
    println("has got suggest cpa")
    val aucDF = getAuc(date, spark)
    println("has got auc")
    calculateIndexValue(date, baseIndexDF, isHiddenEtcIndexDF, suggestCpaDF, aucDF, spark)
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

  // 从base表中获得is_hidden等指标
  def getIsHiddenEtcIndex(date: String, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	unitid,
        |	userid,
        |    (case when ocpc_log like '%IsHiddenOcpc:1%' then 1 else 0 end) as is_hidden,
        |    max(adslot_type) as adslot_type,
        |    max(conversion_goal) as cv_goal,
        |    sum(case when isclick > 0 then price  else 0 end) as charge
        |from
        |	dl_cpc.ocpc_base_unionlog
        |where
        |	`date` = '$preDate'
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adslot_type in (1, 2, 3)
        |and
        |    adsrc = 1
        |and
        |    (charge_type is null or charge_type = 1)
        |group by
        |	unitid,
        |	userid,
        | (case when ocpc_log like '%IsHiddenOcpc:1%' then 1 else 0 end)
      """.stripMargin
    val isHiddenEtcIndexDF = spark.sql(sql)
    isHiddenEtcIndexDF
  }

  // 从suggest表中获得suggest_cpa指标值
  def getSuggestCpa(date: String, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    val sql =
      s"""
        |select
        |	a.unitid,
        |	a.userid,
        |	round(a.cpa * 0.01, 4) as suggest_cpa
        |from
        |	(select
        |		unitid,
        |		userid,
        |		cpa,
        |		row_number() over(partition by unitid, userid order by show desc) as row_num
        |	from
        |		dl_cpc.ocpc_suggest_cpa_recommend_hourly
        |	where
        |		`date` = '$preDate') a
        |where
        |	a.row_num = 1
      """.stripMargin
    val suggestCpaDF = spark.sql(sql)
    suggestCpaDF
  }

  // 构造DataFrame，计算AUC
  def getAuc(date: String, spark: SparkSession): DataFrame ={
    val preDate = getPreDate(date, 1)
    val sql1 =
      s"""
        |select
        |	a.unitid,
        |	a.userid,
        |	1 as conversion_goal,
        |	a.exp_cvr,
        |	b.iscvr1 as iscvr
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
    val dataFrame1 = spark.sql(sql1)

    val sql2 =
      s"""
        |select
        |	a.unitid,
        |	a.userid,
        |	2 as conversion_goal,
        |	a.exp_cvr,
        |	c.iscvr2 as iscvr
        |from
        |	dl_cpc.ocpc_base_unionlog a
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
    val dataFrame2 = spark.sql(sql2)

    val sql3 =
      s"""
        |select
        |	a.unitid,
        |	a.userid,
        |	3 as conversion_goal,
        |	a.exp_cvr,
        |	d.iscvr3 as iscvr
        |from
        |	dl_cpc.ocpc_base_unionlog a
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
    val dataFrame3 = spark.sql(sql3)

    println("dateFrame1:")
    dataFrame1.printSchema()
    println("------------------")
    println("dateFrame2:")
    dataFrame2.printSchema()
    println("------------------")
    println("dateFrame3:")
    dataFrame3.printSchema()

    val dataFrame = dataFrame1.union(dataFrame2).union(dataFrame3)
    println("-----------------")
    println("dateFrame:")
    dataFrame.printSchema()
    val hour = "16"
    val aucDF = OcpcHourlyAucReport.calculateAUCbyUnitid(dataFrame, date, hour, spark)
    aucDF
  }

  // 将ocpc_log_dict中的字符转化成数字
  def convStr2Num(dataDF: DataFrame, spark: SparkSession): DataFrame ={
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
  def calculateIndexValue(date: String, baseIndexDF: DataFrame, isHiddenEtcIndexDF: DataFrame,
                          suggestCpaDF: DataFrame, aucDF: DataFrame, spark: SparkSession): Unit ={
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
        |    / sum(case when isclick = 1 and kvalue is not null then 1 else 0 end), 4) as kvalue
        |from
        |    base_index
        |group by
        |    `date`,
        |    unitid,
        |    userid
      """.stripMargin
    val temp_compIndexValueDF = spark.sql(sql)

    temp_compIndexValueDF.createOrReplaceTempView("temp_comp_index_value")
    isHiddenEtcIndexDF.createOrReplaceTempView("etc_index_value")
    suggestCpaDF.createOrReplaceTempView("suggest_cpa")
    aucDF.createOrReplaceTempView("auc_temp")

    val sql2 =
      s"""
        |select
        |	a.dt,
        |	a.unitid,
        |	a.userid,
        |	a.cpagiven,
        |	a.cpareal1,
        |	a.cpareal2,
        |	a.cpareal3,
        |	a.cpm,
        |	a.arpu,
        |	a.show,
        |	a.click,
        |	a.cv1,
        |	a.cv2,
        |	a.cv3,
        |	a.pre_cvr,
        |	a.post_cvr1,
        |	a.post_cvr2,
        |	a.post_cvr3,
        |	a.acp,
        |	a.bid,
        |	a.acb,
        |	a.kvale,
        |	b.is_hidden,
        |	b.adslot_type,
        |	b.cv_goal,
        |	b.charge,
        |	c.suggest_cpa,
        | d.auc
        |from
        |	temp_comp_index_value a
        |left join
        |	etc_index_value b
        |on
        |	a.unitid = b.unitid
        |and
        |	a.userid = b.userid
        |left join
        |	suggest_cpa c
        |on
        |	a.unitid = c.unitid
        |and
        |	a.userid = c.userid
        |left join
        | auc_temp d
        |on
        | a.unitid = d.unitid
        |and
        | a.userid = d.userid
      """.stripMargin
    val compIndexValueDF = spark.sql(sql2)
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
        | b.is_hidden,
        | b.adslot_type,
        | b.cv_goal,
        | (case when a.conversion_goal = 1 then b.cv1
        |    	  when a.conversion_goal = 2 then b.cv2
        |    	  else b.cv3 end) as cv,
        | b.click,
        | b.show,
        | b.charge,
        | b.pre_cvr,
        | (case when a.conversion_goal = 1 then b.post_cvr1
        |    	  when a.conversion_goal = 2 then b.post_cvr2
        |    	  else b.post_cvr3 end) as post_cvr,
        | b.acp,
        | b.acb,
        | (case when a.conversion_goal = 1 then b.cpareal1
        |    	  when a.conversion_goal = 2 then b.cpareal2
        |    	  else b.cpareal3 end) as cpareal,
        | b.cpagiven,
        | b.auc,
        | b.kvalue,
        | b.cpm,
        | b.arpu,
        |from
        |	pre_ad_info a
        |left join
        |	(select
        |		*
        |	from
        |		dl_cpc.ocpc_aa_base_index_value
        |	where
        |   `dt` between '$startDate' and '$endDate'
        | and
        |   `dt` is not null) as b
        |on
        |	a.unitid = b.unitid
        |where
        | a.conversion_goal = b.conversion_goal
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
