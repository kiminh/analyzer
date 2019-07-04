package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
//import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.ocpcV3.utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val hourInt = args(3).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val resultDF = OcpcCalculateAUCmain(date, hour, version, hourInt, spark)

    resultDF
      .repartition(10)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly_v2")
//      .write.mode("overwrite").insertInto("test.ocpc_unitid_auc_hourly_v2")
  }

  def OcpcCalculateAUCmain(date: String, hour: String, version: String, hourInt: Int, spark: SparkSession) = {
    // 抽取数据
    val data = getData(hourInt, version, date, hour, spark)
    val tableName = "dl_cpc.ocpc_auc_raw_data_v2"
    data
      .repartition(100).write.mode("overwrite").insertInto(tableName)

    // 计算auc
    val aucData = getAuc(tableName, version, date, hour, spark)
    val resultDF = aucData
      .select("identifier", "media", "conversion_goal", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
  }

  def getData(hourInt: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition1 = getTimeRangeSqlDate(date1, hour1, date, hour)

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    cast(unitid as string) identifier,
         |    cast(exp_cvr * 1000000 as bigint) as score,
         |    conversion_goal,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition1
         |and isclick = 1
         |and $mediaSelection
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |and is_ocpc = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    // 取历史区间: cvr数据
    val selectCondition2 = s"`date`>='$date1'"
    // 抽取数据
    val sqlRequest2 =
    s"""
       |SELECT
       |  searchid,
       |  label,
       |  cvr_goal
       |FROM
       |  dl_cpc.ocpc_label_cvr_hourly
       |WHERE
       |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid", "cvr_goal"), "left_outer")
      .select("searchid", "identifier", "media", "conversion_goal", "score", "label", "industry")
      .na.fill(0, Seq("label"))
      .select("searchid", "identifier", "media", "conversion_goal", "score", "label", "industry")
      .withColumn("version", lit(version))

    resultDF
  }



  def getAuc(tableName: String, version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table(tableName)
      .where(s"version='$version'")
    import spark.implicits._

    val newData = data
      .select("identifier", "media", "conversion_goal", "score", "label")
      .withColumn("id", concat_ws("-", col("identifier"), col("media"), col("conversion_goal")))
      .selectExpr("id", "cast(score as int) score", "label")
      .coalesce(400)

    newData.show(10)

    val result = utils.getGauc(spark, newData, "id")
    val resultRDD = result.rdd.map(row => {
      val id = row.getAs[String]("name")
      val identifierList = id.trim.split("-")
      val identifier = identifierList(0)
      val media = identifierList(1)
      val conversionGoal = identifierList(2).toInt
      val auc = row.getAs[Double]("auc")
      (identifier, media, conversionGoal, auc)
    })
    val resultDF = resultRDD.toDF("identifier", "media", "conversion_goal", "auc")
    resultDF
  }


}