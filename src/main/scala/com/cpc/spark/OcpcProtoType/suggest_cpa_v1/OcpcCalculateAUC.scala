package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.ocpcV3.utils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour, $conversionGoal")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData(conversionGoal, 24, version, date, hour, spark)
//    val tableName = "test.ocpc_auc_raw_conversiongoal_" + conversionGoal
//    data
//      .repartition(10).write.mode("overwrite").saveAsTable(tableName)
    val tableName = "dl_cpc.ocpc_auc_raw_data"
    data
      .repartition(10).write.mode("overwrite").insertInto(tableName)

    // 获取identifier与industry之间的关联表
    val unitidIndustry = getIndustry(tableName, conversionGoal, version, date, hour, spark)

    // 计算auc
    val aucData = getAuc(tableName, conversionGoal, version, date, hour, spark)

    val result = aucData
      .join(unitidIndustry, Seq("identifier"), "left_outer")
      .select("identifier", "auc", "industry")

    val conversionGoalInt = conversionGoal.toInt
    val resultDF = result
      .withColumn("conversion_goal", lit(conversionGoalInt))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    val finalTableName = "test.ocpc_unitid_auc_daily_" + conversionGoal
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly")
//        .write.mode("overwrite").saveAsTable(finalTableName)
  }

  def getIndustry(tableName: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    identifier,
         |    industry,
         |    count(distinct searchid) as cnt
         |from $tableName
         |where
         |    conversion_goal = $conversionGoal
         |AND
         |    version = '$version'
         |group by identifier, industry
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |    t.identifier,
         |    t.industry
         |FROM
         |    (SELECT
         |        identifier,
         |        industry,
         |        cnt,
         |        row_number() over(partition by identifier order by cnt desc) as seq
         |    FROM
         |        raw_data) as t
         |WHERE
         |    t.seq=1
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2)

    resultDF
  }

  def getData(conversionGoal: Int, hourInt: Int, version: String, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition1 = getTimeRangeSql2(date1, hour1, date, hour)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    cast(unitid as string) identifier,
         |    cast(exp_cvr * 1000000 as bigint) as score,
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
         |and media_appsid  in ("80000001", "80000002")
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark.sql(sqlRequest)

    // 取历史区间: cvr数据
    val selectCondition2 = s"`date`>='$date1'"
    // 根据conversionGoal选择cv的分区
    val cvrType = "cvr" + conversionGoal.toString
    // 抽取数据
    val sqlRequest2 =
    s"""
       |SELECT
       |  searchid,
       |  label
       |FROM
       |  dl_cpc.ocpc_label_cvr_hourly
       |WHERE
       |  ($selectCondition2)
       |AND
       |  (cvr_goal = '$cvrType')
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid"), "left_outer")
      .select("searchid", "identifier", "score", "label", "industry")
      .na.fill(0, Seq("label"))
      .select("searchid", "identifier", "score", "label", "industry")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("version", lit(version))

    resultDF
  }



  def getAuc(tableName: String, conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table(tableName)
      .where(s"conversion_goal='$conversionGoal' and version='$version'")
    import spark.implicits._

    val newData = data
      .selectExpr("identifier", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "identifier")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val auc = row.getAs[Double]("auc")
      (identifier, auc)
    })
    val resultDF = resultRDD.toDF("identifier", "auc")
    resultDF
  }


}