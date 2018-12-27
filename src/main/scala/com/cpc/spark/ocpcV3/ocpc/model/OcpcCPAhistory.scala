package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCPAhistory {
  def main(args: Array[String]): Unit = {
    /*
    选取cpa_history的基本策略：
    1. 抽取基础表
    2. 分别计算该广告单元在趣头条前三天(根据config决定天数)的历史cpa，以及这几天行业类别的历史cpa
    3. 根据unitid和行业类别关联相关数据
    4. 如果趣头条上至少有一个类别的转化数，按照类别，给定conversion_goal，如果趣头条上一个类别的转化数都没有，conversion_goal默认为1
    5. 按照如下顺序根据转化目标选取合适的cpa：趣头条cpa->行业类别cpa
    6. 输出数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 获取参数
    val conf = ConfigFactory.load("ocpc")
    val days = conf.getInt("ocpc_all.cpa_history.cpa_days")
    val alpha = conf.getDouble("ocpc_all.cpa_history.cpa_alpha")

    // 按照要求生成相关基础数据表
    val baseData = getBaseData(date, hour, days, spark)
    val qttData = getQttCPA(baseData, date, hour, spark)
    val adclassData = getAdclassCPA(baseData, date, hour, spark)
    adclassData.write.mode("overwrite").insertInto("dl_cpc.ocpc_cpa_history_adclass_hourly")
//    adclassData.write.mode("overwrite").saveAsTable("test.ocpc_cpa_history_adclass_hourly")
    val qttAlpha = checkCPAhistory(qttData, alpha, "qtt", date, hour, spark)

    // 数据表关联
    val data = baseData
      .select("unitid", "new_adclass", "conversion_goal")
      .distinct()
      .join(qttAlpha, Seq("unitid", "new_adclass", "conversion_goal"), "left_outer")
      .join(adclassData, Seq("new_adclass", "conversion_goal"), "left_outer")
      .select("unitid", "new_adclass", "conversion_goal", "cpa_history_qtt", "cpa_adclass")

    // 按照策略挑选合适的cpa以及确定对应的conversion_goal
    val result = getResult(data, date, hour, spark)
    val tableName = "dl_cpc.ocpc_cpa_history_hourly"
//    result.write.mode("overwrite").saveAsTable("test.ocpc_cpa_history_hourly")
    result.write.mode("overwrite").insertInto(tableName)
    println(s"save data into table: $tableName")

  }

  def getBaseData(date: String, hour: String, days: Int, spark: SparkSession) = {
    /*
    抽取基础表，只包括前一天在米读小说和趣头条上有记录的unitid和对应adclass
     */
    // 计算日期周期
    val dayDiff = days - 1
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)
    calendar.add(Calendar.DATE, -dayDiff)
    val start_date2 = calendar.getTime
    val date2 = sdf.format(start_date2)
    val selectCondition = s"`date` between '$date2' and '$date1'"

    // 消费
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  total_price as total_cost,
         |  total_bid as total_bid,
         |  ctr_cnt
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest1)
    val costData = spark
      .sql(sqlRequest1)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(
        sum(col("total_cost")).alias("total_cost"),
        sum(col("total_bid")).alias("total_bid"),
        sum(col("ctr_cnt")).alias("ctrcnt"))
      .select("unitid", "new_adclass", "media_appsid", "total_cost", "total_bid", "ctrcnt")

    // 非api回传类转化数(ml_cvr_feature_v1)
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  cvr1_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr1_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark
      .sql(sqlRequest2)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(sum(col("cvr1_cnt")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(1))
      .select("unitid", "new_adclass", "media_appsid", "cvrcnt", "conversion_goal")

    // api回传类转化数(ml_cvr_feature_v2)
    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val cvr2Data = spark
      .sql(sqlRequest3)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(sum(col("cvr2_cnt")).alias("cvrcnt"))
      .withColumn("conversion_goal", lit(2))
      .select("unitid", "new_adclass", "media_appsid", "cvrcnt", "conversion_goal")

    val cvrData = cvr1Data.union(cvr2Data)

    // 关联数据构成基础表
    val resultDF = costData
      .join(cvrData, Seq("unitid", "new_adclass", "media_appsid"), "left_outer")
      .select("unitid", "new_adclass", "media_appsid", "total_cost", "cvrcnt", "total_bid", "ctrcnt", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }

  def getQttCPA(base: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取趣头条cpa数据
     */
    val resultDF = base
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("unitid", "new_adclass", "conversion_goal")
      .agg(
        sum(col("total_cost")).alias("cost"),
        sum(col("cvrcnt")).alias("cvrcnt"),
        sum(col("total_bid")).alias("bid"),
        sum(col("ctrcnt")).alias("ctrcnt"))
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("avg_bid", col("bid") * 1.0 / col("ctrcnt"))
      .withColumn("alpha", col("cpa") * 1.0 / col("avg_bid"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }


  def getAdclassCPA(base: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取趣头条广告的行业类别cpa
     */
    val data = base
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("new_adclass", "conversion_goal")
      .agg(
        sum(col("total_cost")).alias("cost"),
        sum(col("cvrcnt")).alias("cvrcnt"))
      .withColumn("cpa_adclass", col("cost") * 1.0 / col("cvrcnt"))
      .filter("conversion_goal=1")

    val resultDF = data
      .select("new_adclass", "cpa_adclass", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("v1"))

    resultDF
  }

  def checkCPAhistory(base: DataFrame, alpha: Double, media: String, date: String, hour: String, spark: SparkSession) = {
    /*
    根据给定alpha对给定数据的cpa做上限限制（防止因为cvr过低带来的异常值）
     */
    val cvr1Data = base
      .filter("conversion_goal=1")
      .select("unitid", "new_adclass", "cvrcnt", "cpa", "avg_bid", "alpha")
    cvr1Data.createOrReplaceTempView("cvr1_table")
    val cvr2Data = base
      .filter("conversion_goal=2")
      .select("unitid", "new_adclass", "cvrcnt", "cpa", "avg_bid", "alpha")
    cvr2Data.createOrReplaceTempView("cvr2_table")

    // 按照alpha取分位数
    val sqlRequest1 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha, $alpha) as alpha_max
         |FROM
         |  cvr1_table
         |WHERE
         |  cvrcnt > 1
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest1)
    val alpha1Data = spark.sql(sqlRequest1)
    val cvr1alpha = cvr1Data
      .join(alpha1Data, Seq("new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cvrcnt", "cpa", "avg_bid", "alpha", "alpha_max")
      .withColumn("cpa_max", col("avg_bid") * col("alpha_max"))
      .withColumn("cpa_history_" + media, when(col("cpa") > col("cpa_max") && col("cpa_max") > 0, col("cpa_max")).otherwise(col("cpa")))
      .withColumn("conversion_goal", lit(1))
    val cvr1Final = cvr1alpha
      .select("unitid", "new_adclass", "cpa_history_" + media, "conversion_goal")

    val sqlRequest2 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha, $alpha) as alpha_max
         |FROM
         |  cvr2_table
         |WHERE
         |  cvrcnt > 1
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest2)
    val alpha2Data = spark.sql(sqlRequest2)
    val cvr2alpha = cvr2Data
      .join(alpha2Data, Seq("new_adclass"), "left_outer")
      .select("unitid", "new_adclass", "cvrcnt", "cpa", "avg_bid", "alpha", "alpha_max")
      .withColumn("cpa_max", col("avg_bid") * col("alpha_max"))
      .withColumn("cpa_history_" + media, when(col("cpa") > col("cpa_max") && col("cpa_max")>0, col("cpa_max")).otherwise(col("cpa")))
      .withColumn("conversion_goal", lit(2))
    val cvr2Final = cvr2alpha
      .select("unitid", "new_adclass", "cpa_history_" + media, "conversion_goal")

    // 关联数据表
    val resultDF = cvr1Final.union(cvr2Final)



    resultDF
  }

  def getResult(base: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 确定转化目标
    2. 根据转化目标和cpa优先级选择最终cpa
     */
    base.createOrReplaceTempView("base_table")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  new_adclass,
         |  cpa_history_qtt,
         |  cpa_adclass,
         |  conversion_goal,
         |  (case when cpa_history_qtt is null then cpa_adclass
         |        else cpa_history_qtt end) as cpa_history,
         |  (case when cpa_history_qtt is null then 'adclass'
         |        else 'qtt' end) as cpa_src
         |FROM
         |  base_table
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.printSchema()

    val data = rawData
      .withColumn("cpa_history", when(col("cpa_history") > 50000, 50000).otherwise(col("cpa_history")))

    // 对于暗投，pb文件中每个identifier只能有一个conversion goal，在这里进行控制
    val data1 = data
      .filter(s"conversion_goal=1")
      .withColumn("cpa1_history", col("cpa_history"))
      .withColumn("cpa1_src", col("cpa_src"))
      .select("unitid", "new_adclass", "cpa1_history", "cpa1_src")
    val data2 = data.filter(s"conversion_goal=2")
      .filter(s"conversion_goal=2")
      .withColumn("cpa2_history", col("cpa_history"))
      .withColumn("cpa2_src", col("cpa_src"))
      .select("unitid", "new_adclass", "cpa2_history", "cpa2_src")

    val result = data1
      .join(data2, Seq("unitid", "new_adclass"), "outer")
      .withColumn("conversion_goal", when(col("cpa1_history").isNull, 2).otherwise(1))
      .withColumn("cpa_history", when(col("conversion_goal")===1, col("cpa1_history")).otherwise(col("cpa2_history")))
      .withColumn("cpa_src", when(col("conversion_goal")===1, col("cpa1_src")).otherwise(col("cpa2_src")))
      .select("unitid", "new_adclass", "cpa_history", "cpa_src", "conversion_goal")
      .filter("cpa_history is not null")




    result.show(10)

//    data
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .saveAsTable("test.ocpc_cpa_history_middle_hourly")

    val resultDF = result
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "new_adclass", "cpa_src", "cpa_history", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("v1"))

    resultDF

  }

}
