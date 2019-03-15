package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.ocpcV3.utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcCollectSuggestData {
  def main(args: Array[String]): Unit = {
    /*
    从dl_cpc.ocpc_suggest_cpa_recommend_hourly中抽取需要的字段
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession
      .builder()
      .appName(s"OcpcCollectSuggestData: $date, $hour")
      .enableHiveSupport().getOrCreate()

//    // api回传的feedapp广告
//    val feedapp1 = getSuggestData("qtt_demo", "feedapp", 2, 100000, date, hour, spark)
//    val feedapp = feedapp1.withColumn("exp_tag", lit("OcpcHiddenAdv"))

    // 二类电商
    val elds1 = getSuggestData("qtt_demo", "elds", 3, 30000, date, hour, spark)
    val elds = elds1.withColumn("exp_tag", lit("OcpcHiddenAdv"))

    // 从网赚推荐cpa抽取数据
    val wz1 = getSuggestData("wz", "wzcp", 1, 50000, date, hour, spark)
    val wz = wz1.withColumn("exp_tag", lit("OcpcHiddenClassAdv"))

    // 数据串联
    val cpaData = elds
      .union(wz)

    // 整理每个网赚广告单元最近24小时ocpc消费的成本和cpm
    val ocpcData = getOcpcData(date, hour, spark)

    // 整理每个广告单元最近24小时的消费
    val cpcData = getCpcData(date, hour, spark)

    // 数据关联，并计算预算
    val data = joinData(cpaData, cpcData, ocpcData, spark)

    data
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_auto_dark_test20190315")
  }

  def getOcpcData(date: String, hour: String, spark: SparkSession) = {
    /*
    1.  点击数据
    2.  转化数据
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    isclick,
         |    isshow,
         |    price,
         |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden,
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    is_ocpc=1
         |AND
         |    media_appsid  in ("80000001", "80000002")
         |AND
         |    isshow=1
         |AND
         |    antispam = 0
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1).filter(s"is_hidden = 1")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr1
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'wz'
       """.stripMargin
    println(sqlRequest2)
    val cvrDataWZ = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    println(sqlRequest3)
    val cvrDataAPP = spark.sql(sqlRequest3)

    val sqlRequest4 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr3
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest4)
    val cvrDataElds = spark.sql(sqlRequest4)

    // 数据关联
//    searchid,
//    unitid,
//    isclick,
//    isshow,
//    price,
//    cpagiven,
    val data = ctrData
      .join(cvrDataWZ, Seq("searchid"), "left_outer")
      .join(cvrDataAPP, Seq("searchid"), "left_outer")
      .join(cvrDataElds, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "isclick", "isshow", "price", "cpagiven", "iscvr1", "iscvr2", "iscvr3")

    data.createOrReplaceTempView("base_table")
    val sqlRequest5 =
      s"""
         |SELECT
         |  unitid,
         |  round(sum(case WHEN isclick=1 then price else 0 end)*10.0/sum(isshow),3) as ocpc_cpm,
         |  (case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven,
         |  (case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr1) as cpa1,
         |  (case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr2) as cpa2,
         |  (case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr3) as cpa3
         |FROM
         |  base_table
       """.stripMargin
    println(sqlRequest5)
    val result = spark.sql(sqlRequest5)

    val result1 = result
      .withColumn("cpareal", col("cpa1"))
      .withColumn("cpa_ratio", col("cpagiven") * 1.0 / col("cpareal"))
      .withColumn("cpa_flag", when(col("cpa_ratio") > 0.8, 1).otherwise(0))
      .na.fill(0, Seq("cpa_flag"))
      .withColumn("industry", lit("wzcp"))
      .select("unitid", "cpagiven", "cpareal", "industry", "cpa_flag", "ocpc_cpm")

    val result2 = result
      .withColumn("cpareal", col("cpa2"))
      .withColumn("cpa_ratio", col("cpagiven") * 1.0 / col("cpareal"))
      .withColumn("cpa_flag", when(col("cpa_ratio") > 0.8, 1).otherwise(0))
      .na.fill(0, Seq("cpa_flag"))
      .withColumn("industry", lit("feedapp"))
      .select("unitid", "cpagiven", "cpareal", "industry", "cpa_flag", "ocpc_cpm")

    val result3 = result
      .withColumn("cpareal", col("cpa3"))
      .withColumn("cpa_ratio", col("cpagiven") * 1.0 / col("cpareal"))
      .withColumn("cpa_flag", when(col("cpa_ratio") > 0.8, 1).otherwise(0))
      .na.fill(0, Seq("cpa_flag"))
      .withColumn("industry", lit("elds"))
      .select("unitid", "cpagiven", "cpareal", "industry", "cpa_flag", "ocpc_cpm")

    val resultDF = result1.union(result2).union(result3)

    resultDF
  }

  def joinData(cpaData: DataFrame, costData: DataFrame, ocpcData: DataFrame, spark: SparkSession) ={
    val data = cpaData
      .join(costData, Seq("unitid"), "inner")
      .select("unitid", "cpa", "kvalue", "cost", "conversion_goal", "max_budget", "industry", "exp_tag", "userid", "planid", "daily_cost", "cpc_cpm")
      .join(ocpcData, Seq("unitid", "industry"), "left_outer")
      .select("unitid", "cpa", "kvalue", "cost", "conversion_goal", "max_budget", "industry", "exp_tag", "userid", "planid", "daily_cost", "cpagiven", "cpareal", "cpa_flag", "ocpc_cpm")

    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  planid,
         |  cpa,
         |  kvalue,
         |  conversion_goal,
         |  exp_tag,
         |  max_budget,
         |  daily_cost,
         |  (case when cpa_flag = 1 and cpc_cpm < ocpc_cpm then 0.1
         |        else 0.05 end) as percent
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)

    val result = spark.sql(sqlRequest)
      .withColumn("budget_mid", col("daily_cost") * col("percent"))
      .withColumn("budget", when(col("budget_mid") < col("max_budget"), col("budget_mid")).otherwise(col("max_budget")))
    result.show(10)

    val resultDF = result
      .select("unitid", "userid", "planid", "cpa", "kvalue", "conversion_goal", "budget", "exp_tag")

    resultDF

  }

  def getCpcData(date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val mediaSelection = conf.getString("medias.qtt.media_selection")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    planid,
         |    round(sum(case WHEN isclick=1 then price else 0 end)*10.0/sum(isshow),3) as cpc_cpm,
         |    sum(case when isclick = 1 then price else 0 end) as daily_cost
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    $mediaSelection
         |AND
         |    antispam = 0
         |AND
         |    adslot_type in (1,2,3)
         |AND
         |    adsrc = 1
         |AND
         |    (charge_type is null or charge_type = 1)
         |GROUP BY unitid, userid, planid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getSuggestData(version: String, industry: String, conversionGoal: Int, maxBudget: Int, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  cpa,
         |  kvalue,
         |  cost,
         |  row_number() over(partition by unitid order by cost desc) as seq
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  industry = '$industry'
         |AND
         |  original_conversion = $conversionGoal
         |AND
         |  is_recommend = 1
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val resultDF = data
        .filter(s"seq = 1")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("max_budget", lit(maxBudget))
        .withColumn("industry", lit(industry))

    resultDF.show(10)
    resultDF
  }
}
