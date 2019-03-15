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

    // api回传的feedapp广告
    val feedapp1 = getSuggestData("qtt_demo", "feedapp", 2, 100000, date, hour, spark)
    val feedapp = feedapp1.withColumn("exp_tag", lit("OcpcHiddenAdv"))

    // 二类电商
    val elds1 = getSuggestData("qtt_demo", "elds", 3, 30000, date, hour, spark)
    val elds = elds1.withColumn("exp_tag", lit("OcpcHiddenAdv"))

    // 从网赚推荐cpa抽取数据
    val wz1 = getSuggestData("wz", "wzcp", 1, 50000, date, hour, spark)
    val wz = wz1.withColumn("exp_tag", lit("OcpcHiddenClassAdv"))

    // 数据串联
    val cpaData = feedapp
      .union(elds)
      .union(wz)

    // 整理每个广告单元最近24小时的消费
    val costData = getCost(date, hour, spark)

    // 整理每个网赚广告单元最近24小时ocpc消费的成本
    val historyCPA = getHistoryCPA(date, hour, spark)

    // 数据关联，并计算预算
    val data = joinData(cpaData, costData, 0.1, spark)

    data
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_auto_dark_test20190315")
  }

  def getHistoryCPA(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取二类电商暗投数据，并计算cpa_ratio
     */
  }

  def joinData(cpaData: DataFrame, costData: DataFrame, percent: Double, session: SparkSession) ={
    val data = cpaData
      .join(costData, Seq("unitid"), "inner")
      .select("unitid", "cpa", "kvalue", "cost", "conversion_goal", "max_budget", "exp_tag", "userid", "planid", "daily_cost")

    val result = data
      .withColumn("budget_mid", col("daily_cost") * percent)
      .withColumn("budget", when(col("budget_mid") < col("max_budget"), col("budget_mid")).otherwise(col("max_budget")))
    result.show(10)

    val resultDF = result
      .select("unitid", "userid", "planid", "cpa", "kvalue", "conversion_goal", "budget", "exp_tag")

    resultDF

  }

  def getCost(date: String, hour: String, spark: SparkSession) = {
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

    resultDF.show(10)
    resultDF
  }
}
