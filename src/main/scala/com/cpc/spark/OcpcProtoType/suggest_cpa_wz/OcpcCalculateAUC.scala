package com.cpc.spark.OcpcProtoType.suggest_cpa_wz

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql3
import com.cpc.spark.ocpcV3.utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc unitid auc: $date, $hour, $conversionGoal")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData(conversionGoal, version, date, hour, spark)
    val tableName = "test.ocpc_auc_raw_conversiongoal_" + conversionGoal
    data
      .repartition(10).write.mode("overwrite").saveAsTable(tableName)
//    data
//      .repartition(10).write.mode("overwrite").insertInto(tableName)

    // 获取unitid与industry之间的关联表
    val unitidIndustry = getIndustry(date, hour, spark)

    // 计算auc
    val aucData = getAuc(tableName, conversionGoal, version, date, hour, spark)

    val result = aucData
      .join(unitidIndustry, Seq("unitid"), "left_outer")
      .select("unitid", "auc", "industry")

    val conversionGoalInt = conversionGoal.toInt
    val resultDF = result
      .withColumn("conversion_goal", lit(conversionGoalInt))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    val finalTableName = "test.ocpc_unitid_auc_daily_" + conversionGoal
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_daily")
//        .write.mode("overwrite").saveAsTable(finalTableName)
  }

  def getIndustry(date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition1 = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |select
         |    unitid,
         |    industry,
         |    count(distinct searchid) as cnt
         |from dl_cpc.slim_union_log
         |where $selectCondition1
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

  def getData(conversionGoal: String, version: String, date: String, hour: String, spark: SparkSession) = {
    //    // 取历史区间: score数据
    //    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    //    val today = dateConverter.parse(date)
    //    val calendar = Calendar.getInstance
    //    calendar.setTime(today)
    ////    calendar.add(Calendar.DATE, 2)
    ////    val yesterday = calendar.getTime
    ////    val date1 = dateConverter.format(yesterday)
    ////    val selectCondition1 = s"`date`='$date1'"
    //    val selectCondition1 = s"`dt`='$date'"
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
    val selectCondition1 = getTimeRangeSql3(date1, hour1, date, hour)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    exp_cvr as score
         |from dl_cpc.slim_union_log
         |where $selectCondition1
         |and isclick = 1
         |and media_appsid  in ("80000001", "80000002")
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark.sql(sqlRequest)

    // 取历史区间: cvr数据
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.qtt.cv_pt.cvr" + conversionGoal
    val cvrGoal = conf.getString(conf_key)
    println(s"conf key is: $conf_key")
    println(s"cvr partition is: $cvrGoal")
    val selectCondition2 = s"`date`>='$date1'"
    // 根据conversionGoal选择cv的sql脚本
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
       |  (cvr_goal = '$cvrGoal')
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "score", "label")
      .na.fill(0, Seq("label"))
      .select("searchid", "unitid", "score", "label")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
  }

  def filterData(tableName: String, cvThreshold: Int, conversionGoal: String, version: String, date: String, hour: String, spark: SparkSession) = {
    val rawData = spark
      .table(tableName)
      .where(s"`date`='$date' and conversion_goal='$conversionGoal' and version='$version'")

    val filterCondition = s"when conversion_goal is $conversionGoal: cvrcnt >= $cvThreshold"
    println("############ filter function #######################")
    println(filterCondition)
    val dataIdea = rawData
      .groupBy("userid")
      .agg(sum(col("label")).alias("cvrcnt"))
      .select("userid", "cvrcnt")
      .filter(s"cvrcnt >= $cvThreshold")

    val resultDF = rawData
      .join(dataIdea, Seq("userid"), "inner")
      .select("searchid", "userid", "score", "label")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
  }


  def getAuc(tableName: String, conversionGoal: String, version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table(tableName)
      .where(s"`date`='$date' and conversion_goal='$conversionGoal' and version='$version'")
    import spark.implicits._

    val newData = data
      .selectExpr("cast(unitid as string) unitid", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "unitid")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val auc = row.getAs[Double]("auc")
      (identifier, auc)
    })
    val resultDF = resultRDD.toDF("unitid", "auc")
    resultDF
  }


}