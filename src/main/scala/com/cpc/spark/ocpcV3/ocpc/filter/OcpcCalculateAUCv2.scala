package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.{getTimeRangeSql2, getTimeRangeSql3}
import com.cpc.spark.ocpcV3.utils
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OcpcCalculateAUCv2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toString
    val version = args(3).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc userid auc: $date, $hour, $conversionGoal")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData(conversionGoal, version, date, hour, spark)
    // val tableName1 = "test.ocpc_auc_raw_conversiongoal_bak_" + conversionGoal.toString
    val tableName1 = "dl_cpc.ocpc_auc_raw_conversiongoal"
    data
      // .repartition(10).write.mode("overwrite").saveAsTable(tableName1)
     .repartition(10).write.mode("overwrite").insertInto(tableName1)

    // 获取userid与industry之间的关联表
    val useridIndustry = getIndustry(date, hour, spark)

    // 计算auc
    val aucData = getAuc(tableName1, conversionGoal, version, date, hour, spark)

    val result = aucData
      .join(useridIndustry, Seq("userid"), "left_outer")
      .select("userid", "auc", "industry")

    val resultDF = result
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
    //    test.ocpc_check_auc_data20190104_bak
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_userid_auc_daily_v2")
//        resultDF.write.mode("overwrite").saveAsTable("test.ocpc_userid_auc_daily_v2")
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
         |    userid,
         |    industry,
         |    count(distinct searchid) as cnt
         |from dl_cpc.slim_union_log
         |where $selectCondition1
         |and isclick = 1
         |and media_appsid  in ("80000001", "80000002")
         |and ideaid > 0 and adsrc = 1
         |and userid > 0
         |group by userid, industry
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |    t.userid,
         |    t.industry
         |FROM
         |    (SELECT
         |        userid,
         |        industry,
         |        cnt,
         |        row_number() over(partition by userid order by cnt desc) as seq
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
         |    userid,
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

    //    // 取历史区间: cvr数据
    //    calendar.add(Calendar.DATE, 2)
    //    val secondDay = calendar.getTime
    //    val date2 = dateConverter.format(secondDay)
    val selectCondition2 = s"`date`>='$date1'"
    // 根据conversionGoal选择cv的sql脚本
    val cvrPt = "cvr" + conversionGoal.toString
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition2
         |AND
         |  cvr_goal = '$cvrPt'
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid"), "left_outer")
      .select("searchid", "userid", "score", "label")
      .na.fill(0, Seq("label"))
      .select("searchid", "userid", "score", "label")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
    //    resultDF.show(10)
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
      .selectExpr("cast(userid as string) userid", "cast(score as int) score", "label")
      .coalesce(400)

    val result = utils.getGauc(spark, newData, "userid")
    val resultRDD = result.rdd.map(row => {
      val identifier = row.getAs[String]("name")
      val auc = row.getAs[Double]("auc")
      (identifier, auc)
    })
    val resultDF = resultRDD.toDF("userid", "auc")
    resultDF
  }

  //  def getAuc(tableName: String, conversionGoal: String, version: String, date: String, hour: String, spark: SparkSession) = {
  //    import spark.implicits._
  //    //获取模型标签
  //
  //    val data = spark
  //      .table(tableName)
  //      .where(s"`date`='$date' and conversion_goal='$conversionGoal' and version='$version'")
  //
  //
  //    val aucList = new mutable.ListBuffer[(String, Double)]()
  //    val useridList = data.select("userid").distinct().cache()
  //    val useridCnt = useridList.count()
  //    data.printSchema()
  //    println(s"################ count of userid list: $useridCnt ################")
  //
  //    //按userid遍历
  //    var cnt = 0
  //    for (row <- useridList.collect()) {
  //      val userid = row.getAs[Int]("userid").toString
  //      println(s"############### userid=$userid, cnt=$cnt ################")
  //      cnt += 1
  //      val userData = data.filter(s"userid=$userid")
  //      val scoreAndLabel = userData
  //        .select("score", "label")
  //        .rdd
  //        .map(x=>(x.getAs[Long]("score").toDouble, x.getAs[Int]("label").toDouble))
  //      val scoreAndLabelNum = scoreAndLabel.count()
  //      if (scoreAndLabelNum > 0) {
  //        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
  //        val aucROC = metrics.areaUnderROC
  //        aucList.append((userid, aucROC))
  //
  //      }
  //    }
  //
  //    useridList.unpersist()
  //    val resultDF = spark
  //      .createDataFrame(aucList)
  //      .toDF("userid", "auc")
  //
  //    resultDF
  //  }
}