package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils._
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object OcpcSuggestCpa{
  def main(args: Array[String]): Unit = {
    //TODO
    // 预估cvr分布，计算进入ocpc之后的展现量
    // 使用cpa_suggest * kvalue * exp_cvr * exp_ctr 作为ecpm计算分布

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc cpc stage data: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 计算costData和cvrData
    val costData = getCost(date, hour, spark)
    val cvr1Data = getCVRv2(1, date, hour, spark)
    val cvr2Data = getCVRv2(2, date, hour, spark)
    val cvr3Data = getCVRv2(3, date, hour, spark)

    val cpa1 = calculateCPA(costData, cvr1Data, date, hour, spark)
    val cpa2 = calculateCPA(costData, cvr2Data, date, hour, spark)
    val cpa3 = calculateCPA(costData, cvr3Data, date, hour, spark)

    // 读取auc数据表
    val aucData = getAUC(version, date, hour, spark)


    // 读取k值数据
    val kvalue = getPbKv2(date, hour, spark)

    // unitid维度的industry
    val unitidIndustry = getIndustry(date, hour, spark)

    // unitid维度判断是否已投ocpc
    val unitidOcpc = getOcpcFlag(date, hour, spark)

    // 判断user的usertype
    val userTypes = getUserType(date, hour, spark)


    // 调整字段
    val cpa1Data = cpa1.withColumn("conversion_goal", lit(1))
    val cpa2Data = cpa2.withColumn("conversion_goal", lit(2))
    val cpa3Data = cpa3.withColumn("conversion_goal", lit(3))

    // 更新pre_cvr, post_cvr的计算规则
    val pcvrData1 = getPreCvr(1, date, hour, spark)
    val pcvrData2 = getPreCvr(2, date, hour, spark)
    val pcvrData3 = getPreCvr(3, date, hour, spark)
    val pcvrData = pcvrData1.union(pcvrData2).union(pcvrData3)

    val cpaDataRaw = cpa1Data
      .union(cpa2Data)
      .union(cpa3Data)
      .select("unitid", "userid", "adclass", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid")
      .join(pcvrData, Seq("unitid", "conversion_goal"), "left_outer")
      .select("unitid", "userid", "adclass", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "pre_cvr", "post_cvr_real", "exp_cvr", "click_new", "conversion")
//    cpaDataRaw.write.mode("overwrite").saveAsTable("test.check_ocpc_k_middle20190131b")

//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))

    val cpaData = cpaDataRaw
      .withColumn("pcvr", col("pre_cvr"))
      .withColumn("post_cvr", col("post_cvr_real"))
      .withColumn("pcoc", col("pcvr") * 1.0 / col("post_cvr"))

    // 检查模型
    val modelData = checkModelPCOC(date, hour, spark)


    val result1 = cpaData
      .join(aucData, Seq("userid", "conversion_goal"), "left_outer")
      .select("unitid", "userid", "adclass", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc")
      .withColumn("original_conversion", col("conversion_goal"))
      .withColumn("conversion_goal", when(col("conversion_goal") === 3, 1).otherwise(col("conversion_goal")))
      .join(kvalue, Seq("unitid", "conversion_goal"), "left_outer")
      .withColumn("cal_bid", col("cpa") * col("pcvr") * col("kvalue") / col("jfb"))
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue")
      .withColumn("is_recommend", when(col("auc").isNotNull && col("auc")>0.65, 1).otherwise(0))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cvrcnt") < 60, 0).otherwise(col("is_recommend")))
      .join(unitidIndustry, Seq("unitid"), "left_outer")
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend")
      .join(unitidOcpc, Seq("unitid"), "left_outer")
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag")
      .na.fill(0, Seq("ocpc_flag"))
      .join(userTypes, Seq("userid"), "left_outer")
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype")
      .join(modelData, Seq("unitid", "userid"), "left_outer")
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2")
      .na.fill(-1, Seq("pcoc1", "pcoc2"))
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))

    val alpha = 0.1
    val result2 = predictOcpcBid(result1, alpha, date, hour, spark)
    val resultDF = result2
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

//    test.ocpc_suggest_cpa_recommend_hourly20190104
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly20190104")
//    resultDF
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly")

  }

  def predictOcpcBid(suggestData: DataFrame, alpha: Double, date: String, hour: String, spark: SparkSession) = {
    /*
    根据slim unionlog抽取数据,并根据cpa，校准cvr，k值计算dynamicbid分布
     */
    // 从slim_union_log抽取数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isclick,
         |  isshow,
         |  exp_cvr * 1.0 / 1000000 as exp_cvr,
         |  bid,
         |  price
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  $selectCondition
         |AND
         |    media_appsid  in ('80000001', '80000002')
         |AND
         |    isshow=1
         |AND antispam = 0
         |AND ideaid > 0
         |AND adsrc = 1
         |AND adslot_type in (1,2,3)
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    // 数据关联
    val data = rawData
      .join(suggestData, Seq("unitid"), "inner")
      .select("searchid", "unitid", "adclass", "original_conversion", "acb", "cpa", "post_cvr", "kvalue", "isclick", "isshow", "exp_cvr", "bid", "price", "jfb")
      .withColumn("cali_cvr", col("exp_cvr") * (1 - alpha) + col("post_cvr") * alpha)
      .withColumn("dynamicbid", col("cali_cvr") * col("cpa") * col("kvalue") * 1.0 / col("jfb"))
//    data.write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_20190221")

    // 统计dynamicbid的数据分布
    data.createOrReplaceTempView("base_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  original_conversion,
         |  sum(case when dynamicbid < 1 then 1 else 0 end) * 1.0 / sum(isshow) as zerobid_percent,
         |  sum(case when dynamicbid >= 1 and dynamicbid < 0.5 * acb then 1 else 0 end) * 1.0 / sum(isshow) as bottom_halfbid_percent,
         |  sum(case when dynamicbid >= 0.5 * acb and dynamicbid < acb then 1 else 0 end) * 1.0 / sum(isshow) as top_halfbid_percent,
         |  sum(case when dynamicbid >= acb then 1 else 0 end) * 1.0 / sum(isshow) as largebid_percent
         |FROM
         |  base_data
         |GROUP BY unitid, original_conversion
       """.stripMargin
    println(sqlRequest2)
    val result = spark.sql(sqlRequest2)

    // 数据关联
    val resultDF = suggestData
      .join(result, Seq("unitid", "original_conversion"), "left_outer")
      .selectExpr("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "cast(zerobid_percent as double) zerobid_percent", "cast(bottom_halfbid_percent as double) bottom_halfbid_percent", "cast(top_halfbid_percent as double) top_halfbid_percent", "cast(largebid_percent as double) largebid_percent")

    resultDF

  }

  def getCVRv2(conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql2(date1, hour1, date, hour)

    // ctrData
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    adclass,
         |    isclick
         |FROM
         |    dl_cpc.slim_union_log
         |WHERE
         |    $selectCondition
         |AND
         |    media_appsid  in ('80000001', '80000002')
         |AND
         |    isclick=1
         |AND antispam = 0
         |AND ideaid > 0
         |AND adsrc = 1
         |AND adslot_type in (1,2,3)
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1)

    // cvrData1
    // 根据conversionGoal选择cv的sql脚本
    var sqlRequest2 = ""
    if (conversionGoal == 1) {
      // cvr1数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition2
           |AND
           |  label2=1
           |AND
           |  label_type in (1, 2, 3, 4, 5)
           |GROUP BY searchid
       """.stripMargin
    } else if (conversionGoal == 2) {
      // cvr2数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition2
           |AND
           |  label=1
           |GROUP BY searchid
       """.stripMargin
    } else {
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.site_form_unionlog
           |WHERE
           |  $selectCondition2
           |GROUP BY searchid
       """.stripMargin
    }
    println(sqlRequest2)
    val cvrRaw = spark.sql(sqlRequest2)

    val cvrData = ctrData
      .join(cvrRaw, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "adclass", "isclick", "iscvr")
      .na.fill(0, Seq("iscvr"))


    // conversiongoal=1
    val resultDF = cvrData
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cvrcnt")
      )
      .select("unitid", "adclass", "cvrcnt")

    resultDF
  }

  def checkModelPCOC(date: String, hour: String, spark: SparkSession) = {
    /*
    检查模型前后的pcoc，目前只覆盖二类电商
     */
    // 检查更新前模型的pcoc
    val sqlRequest1 =
      s"""
         |select
         |    A.unitid as unitid,
         |    A.userid as userid,
         |    sum(A.click) as click,
         |    sum(A.conversion) as conversion,
         |    avg(A.raw_cvr) as pre_cvr,
         |    avg(A.conversion) as post_cvr,
         |    avg(abs(A.raw_cvr - A.conversion)) as mae,
         |    avg(A.raw_cvr) * 1.0 / avg(A.conversion) as pcoc1
         |from
         |    (
         |        select
         |            *
         |        from
         |            dl_cpc.ocpc_report_detail
         |        where
         |            dt='$date'
         |            and pt='v1'
         |            and click>0
         |            and industry='elds'
         |    ) A
         |group by
         |    A.unitid, A.userid
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    // 检查更新后模型的pcoc
    val sqlRequest2 =
      s"""
         |SELECT
         |    t.unitid,
         |    t.userid,
         |    AVG(t.exp_cvr) as pre_cvr,
         |    SUM(t.conversion) as post_cvr,
         |    AVG(t.exp_cvr) * 1.0 / AVG(t.conversion) as pcoc2
         |FROM
         |    (SELECT
         |        unitid,
         |        userid,
         |        click,
         |        conversion,
         |        exp_cvr
         |    FROM
         |        dl_cpc.ocpc_report_about_pcoc
         |    WHERE
         |        dt='$date'
         |    AND
         |        industry='elds'
         |    AND
         |        click=1) as t
         |GROUP BY t.unitid, t.userid
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val resultDF = data1
      .join(data2, Seq("unitid", "userid"), "outer")
      .select("unitid", "userid", "pcoc1", "pcoc2")

    resultDF
  }

  def getPreCvr(conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    按新的标准计算pcvr和postcvr
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql2(date1, hour1, date, hour)

    // ctrData
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    exp_cvr * 1.0 / 1000000 as exp_cvr,
         |    isclick
         |FROM
         |    dl_cpc.slim_union_log
         |WHERE
         |    $selectCondition
         |AND
         |    media_appsid  in ('80000001', '80000002')
         |AND
         |    isclick=1
         |AND antispam = 0
         |AND ideaid > 0
         |AND adsrc = 1
         |AND adslot_type in (1,2,3)
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1)

    // cvrData1
    // 根据conversionGoal选择cv的sql脚本
    var sqlRequest2 = ""
    if (conversionGoal == 1) {
      // cvr1数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition2
           |AND
           |  label2=1
           |AND
           |  label_type in (1, 2, 3, 4, 5)
           |GROUP BY searchid
       """.stripMargin
    } else if (conversionGoal == 2) {
      // cvr2数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition2
           |AND
           |  label=1
           |GROUP BY searchid
       """.stripMargin
    } else {
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as iscvr
           |FROM
           |  dl_cpc.site_form_unionlog
           |WHERE
           |  $selectCondition2
           |GROUP BY searchid
       """.stripMargin
    }
    println(sqlRequest2)
    val cvrRaw = spark.sql(sqlRequest2)

    val cvrData = ctrData
      .join(cvrRaw, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "exp_cvr", "isclick", "iscvr")
      .na.fill(0, Seq("iscvr"))


    // conversiongoal=1
    val data = cvrData
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
      .select("unitid", "post_cvr", "post_cvr_cali")

    val caliData = data
      .join(cvrData, Seq("unitid"), "left_outer")
      .select("searchid", "unitid", "exp_cvr", "isclick", "iscvr", "post_cvr", "post_cvr_cali")
      .withColumn("pre_cvr", when(col("exp_cvr")> col("post_cvr_cali"), col("post_cvr_cali")).otherwise(col("exp_cvr")))
      .select("searchid", "unitid", "exp_cvr", "isclick", "iscvr", "post_cvr", "pre_cvr", "post_cvr_cali")

    val finalData = caliData
      .groupBy("unitid")
      .agg(
        sum(col("pre_cvr")).alias("pre_cvr"),
        sum(col("exp_cvr")).alias("exp_cvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("pre_cvr", col("pre_cvr") * 1.0 / col("click"))
      .withColumn("exp_cvr", col("exp_cvr") * 1.0 / col("click"))
      .select("unitid", "pre_cvr", "exp_cvr", "click", "conversion")

    val resultDF = finalData
      .join(data, Seq("unitid"), "outer")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("post_cvr_real", col("post_cvr"))
      .withColumn("click_new", col("click"))
      .select("unitid", "exp_cvr", "pre_cvr", "post_cvr_real", "conversion_goal", "click_new", "conversion")

    resultDF
  }

  def getUserType(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  userid,
         |  usertype,
         |  count(1) as cnt
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ('80000001', '80000002')
         |GROUP BY userid, usertype
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data.createOrReplaceTempView("base_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |    t.userid,
         |    t.usertype
         |FROM
         |    (SELECT
         |        userid,
         |        usertype,
         |        cnt,
         |        row_number() over(partition by userid order by cnt desc) as seq
         |    FROM
         |        base_data) as t
         |WHERE
         |    t.seq=1
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2)

    resultDF
  }

  def getOcpcFlag(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_cpa_given_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("unitid")
      .withColumn("ocpc_flag", lit(1))
      .distinct()

    data
  }

  def getIndustry(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |select
         |    unitid,
         |    industry,
         |    count(distinct searchid) as cnt
         |from dl_cpc.slim_union_log
         |where $selectCondition
         |and isclick = 1
         |and media_appsid  in ("80000001", "80000002")
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |group by unitid, industry
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |    t.unitid,
         |    t.industry
         |FROM
         |    (SELECT
         |        unitid,
         |        industry,
         |        cnt,
         |        row_number() over(partition by unitid order by cnt desc) as seq
         |    FROM
         |        raw_data) as t
         |WHERE
         |    t.seq=1
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2)

    resultDF
  }

  def getAUC(version: String, date: String, hour: String, spark: SparkSession) = {
    val auc1Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily_v2")
      .where(s"`date`='$date' and version='$version' and conversion_goal='1'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(1))

    val auc2Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily_v2")
      .where(s"`date`='$date' and version='$version' and conversion_goal='2'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(2))

    val auc3Data = spark
      .table("dl_cpc.ocpc_userid_auc_daily_v2")
      .where(s"`date`='$date' and version='$version' and conversion_goal='3'")
      .select("userid", "auc")
      .withColumn("conversion_goal", lit(3))

    val resultDF = auc1Data.union(auc2Data).union(auc3Data)
    resultDF
  }

  def getPbKv2(date: String, hour: String, spark: SparkSession) = {
    /*
    基于unitid维度的新版k值计算方法，从dl_cpc.ocpc_prev_pb_once中抽取，由于有多个conversion_goal,需要进行关联
     */

    // 获取kvalue
    //    ocpc_prev_pb_once
    val resultDF = spark
      .table("dl_cpc.ocpc_prev_pb_once")
      .where(s"version = 'qtt_demo'")
      .withColumn("unitid", col("kvalue"))
      .withColumn("original_conversion", col("conversion_goal"))
      .select("unitid", "kvalue", "original_conversion")

    resultDF
  }

  def getPbK(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -7)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql(date1, hour, date, hour)

    // 关联ideaid与unitid
    val data = spark
      .table("dl_cpc.ocpc_ctr_data_hourly")
      .where(selectCondition)
      .select("ideaid", "unitid")
      .distinct()

    // 获取kvalue
//    ocpc_qtt_prev_pb
//    ocpc_pb_result_table_v7
//    ocpc_qtt_prev_pb20190129
    val kvalue1 = spark
      .table("dl_cpc.ocpc_qtt_prev_pb")
//      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "kvalue1")
      .join(data, Seq("ideaid"), "inner")
      .select("unitid", "kvalue1")
      .groupBy("unitid")
      .agg(avg(col("kvalue1")).alias("kvalue"))
      .select("unitid", "kvalue")
      .withColumn("conversion_goal", lit(1))

    val kvalue2 = spark
      .table("dl_cpc.ocpc_qtt_prev_pb")
//      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "kvalue2")
      .join(data, Seq("ideaid"), "inner")
      .select("unitid", "kvalue2")
      .groupBy("unitid")
      .agg(avg(col("kvalue2")).alias("kvalue"))
      .select("unitid", "kvalue")
      .withColumn("conversion_goal", lit(2))

    val resultDF = kvalue1.union(kvalue2).select("unitid", "kvalue", "conversion_goal")

    resultDF
  }

  def getCost(date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
    val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  show_cnt,
         |  ctr_cnt,
         |  total_price,
         |  total_bid,
         |  total_pcvr
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println("############## getCost function ###############")
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .groupBy("unitid", "userid", "adclass")
      .agg(sum(col("show_cnt")).alias("show"),
        sum(col("ctr_cnt")).alias("click"),
        sum(col("total_price")).alias("cost"),
        sum(col("total_bid")).alias("click_bid_sum"),
        sum(col("total_pcvr")).alias("click_pcvr_sum"))
      .select("unitid", "userid", "adclass", "show", "click", "cost", "click_bid_sum", "click_pcvr_sum")

    resultDF
  }

  def getCVR(cvrType: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
	  val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val tableName = "dl_cpc.ocpcv3_" + cvrType + "_data_hourly"
    println(s"table name is: $tableName")
    val resultDF = spark
      .table(tableName)
      .where(selectCondition)
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("unitid", "adclass")
      .agg(sum(col(cvrType + "_cnt")).alias("cvrcnt"))
      .select("unitid", "adclass", "cvrcnt")


    resultDF
  }

  def calculateCPA(costData: DataFrame, cvrData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = costData
      .join(cvrData, Seq("unitid", "adclass"), "inner")
      .na.fill(0, Seq("cvrcnt"))
      .withColumn("post_ctr", col("click") * 1.0 / col("show"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acb", col("click_bid_sum") * 1.0 / col("click"))
      .withColumn("jfb", col("cost") * 1.0 / col("click_bid_sum"))
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("pcvr", col("click_pcvr_sum") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cvrcnt") * 1.0 / col("click"))
      .withColumn("cal_bid", col("cost") * 1.0 / col("cvrcnt") * (col("click_pcvr_sum") * 1.0 / col("click")))
      .withColumn("pcoc", col("click_pcvr_sum") * 1.0 / col("cvrcnt"))
      .select("unitid", "userid", "adclass", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "cal_bid", "pcoc")

    resultDF
  }

}
