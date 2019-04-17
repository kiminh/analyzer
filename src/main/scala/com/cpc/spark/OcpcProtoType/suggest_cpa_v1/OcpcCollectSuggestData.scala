package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcCollectSuggestData {
  def main(args: Array[String]): Unit = {
    /*
    从dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2中抽取需要的字段
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession
      .builder()
      .appName(s"OcpcCollectSuggestData: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 安装类feedapp广告单元
    val feedapp1 = getSuggestData("hottopic_test", "feedapp", 1, 100000, date, hour, spark)

    feedapp1.printSchema()

    // 数据串联
    val cpaData = feedapp1


    // 整理每个网赚广告单元最近24小时ocpc消费的成本和cpm
    val ocpcData = getOcpcData(date, hour, spark)

    // 整理每个广告单元最近24小时的消费
    val cpcData = getCpcData(date, hour, spark)

    // 关联前一天的自动预算数据表
    val prevBudget = getPrevAutoBudget(date, hour, spark)

    // 数据关联，并计算预算
    val data = joinData(cpaData, cpcData, ocpcData, prevBudget, spark)

    data
      .repartition(5)
//      .write.mode("overwrite").saveAsTable("test.ocpc_auto_budget_once")
      .write.mode("overwrite").saveAsTable("dl_cpc.ocpc_auto_budget_once")

    data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("verion", lit("qtt_demo"))
      .repartition(5)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_auto_budget_hourly")
  }

  def getPrevAutoBudget(date: String, hour: String, spark: SparkSession) = {
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

//    val prevData = spark
//      .table("dl_cpc.ocpc_auto_budget_hourly")
//      .where(s"`date` = '$date1' and `hour` = '$hour1' and version = 'qtt_demo'")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  industry,
         |  conversion_goal,
         |  budget_percent as prev_percent
         |FROM
         |  dl_cpc.ocpc_auto_budget_hourly
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '$hour1'
         |AND
         |  version = 'qtt_demo'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
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
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden
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
    val data = ctrData
      .join(cvrDataWZ, Seq("searchid"), "left_outer")
      .join(cvrDataAPP, Seq("searchid"), "left_outer")
      .join(cvrDataElds, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "isclick", "isshow", "price", "cpagiven", "iscvr1", "iscvr2", "iscvr3")

    val data1 = data
      .filter(s"isclick=1")
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("cpagiven")).alias("cpagiven"),
        sum(col("price")).alias("cost"),
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2"),
        sum(col("iscvr3")).alias("cv3")
      )
      .select("unitid", "click", "cpagiven", "cost", "cv1", "cv2", "cv3")

    val data2 = data
      .groupBy("unitid")
      .agg(sum(col("isshow")).alias("show"))
      .select("unitid", "show")


    val result = data1
      .join(data2, Seq("unitid"), "left_outer")
      .select("unitid", "click", "cpagiven", "cost", "show", "cv1", "cv2", "cv3")
      .withColumn("ocpc_cpm", col("cost") * 10.0 / col("show"))
      .withColumn("cpagiven", col("cpagiven") * 1.0 / col("click"))
      .withColumn("cpa1", col("cost") * 1.0 / col("cv1"))
      .withColumn("cpa2", col("cost") * 1.0 / col("cv2"))
      .withColumn("cpa3", col("cost") * 1.0 / col("cv3"))
      .select("unitid", "ocpc_cpm", "cpagiven", "cpa1", "cpa2", "cpa3")

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

  def joinData(cpaData: DataFrame, costData: DataFrame, ocpcData: DataFrame, prevBudget: DataFrame, spark: SparkSession) ={
    val data = cpaData
      .join(costData, Seq("unitid"), "inner")
      .select("unitid", "cpa", "kvalue", "cost", "last_bid", "conversion_goal", "max_budget", "industry", "exp_tag", "userid", "planid", "daily_cost", "cpc_cpm")
      .join(ocpcData, Seq("unitid", "industry"), "left_outer")
      .select("unitid", "cpa", "kvalue", "cost", "conversion_goal", "max_budget", "industry", "exp_tag", "userid", "planid", "daily_cost", "cpc_cpm", "cpagiven", "cpareal", "cpa_flag", "ocpc_cpm", "last_bid")
      .join(prevBudget, Seq("unitid", "industry", "conversion_goal"), "left_outer")
      .select("unitid", "cpa", "kvalue", "cost", "conversion_goal", "max_budget", "industry", "exp_tag", "userid", "planid", "daily_cost", "cpc_cpm", "cpagiven", "cpareal", "cpa_flag", "ocpc_cpm", "prev_percent", "last_bid")
      .withColumn("top_percent", when(col("industry") === "wzcp", 0.6).otherwise(when(col("industry") === "feedapp", 0.3).otherwise(0.2)))
      .withColumn("bottom_percent", when(col("industry") === "wzcp", 0.3).otherwise(when(col("industry") === "feedapp", 0.1).otherwise(0.05)))

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
         |  industry,
         |  exp_tag,
         |  max_budget,
         |  daily_cost,
         |  cpa_flag,
         |  cpc_cpm,
         |  ocpc_cpm,
         |  prev_percent,
         |  top_percent,
         |  bottom_percent,
         |  (case when prev_percent is not null and cpa_flag = 1 then prev_percent + 0.05
         |        when prev_percent is null or cpa_flag is null then bottom_percent
         |        else prev_percent end) as percent,
         |  last_bid
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)

    val result = spark.sql(sqlRequest)
      .withColumn("budget_percent", when(col("percent") > col("top_percent"), col("top_percent")).otherwise(col("percent")))
      .withColumn("budget_percent", when(col("budget_percent") < col("bottom_percent"), col("bottom_percent")).otherwise(col("budget_percent")))
      .withColumn("budget", col("daily_cost") * col("budget_percent"))

    result.show(10)
    result.write.mode("overwrite").saveAsTable("test.check_data_percent20190315")

    val resultDF = result
      .select("unitid", "userid", "planid", "cpa", "kvalue", "conversion_goal", "budget", "exp_tag", "industry", "budget_percent", "last_bid")

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
         |  cast(0.5 * acb as int) as last_bid,
         |  row_number() over(partition by unitid order by cost desc) as seq
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  industry = '$industry'
         |AND
         |  cv_goal = $conversionGoal
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
