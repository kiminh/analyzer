package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OcpcCalculateAUC {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc userid auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

//    // 抽取数据
//    val data = getData(conversionGoal, version, date, hour, spark)
    val tableName1 = "dl_cpc.ocpc_auc_raw_conversiongoal"
////    data.write.mode("overwrite").saveAsTable(tableName1)
//    data.write.mode("overwrite").insertInto(tableName1)

    // 过滤去除当天cvrcntt<100的userid
    val cvThreshold = 100
    val processedData = filterData(tableName1, cvThreshold, conversionGoal, version, date, hour, spark)
    val tableName2 = "dl_cpc.ocpc_auc_filter_conversiongoal"
//    processedData.write.mode("overwrite").saveAsTable(tableName2)
    processedData.write.mode("overwrite").insertInto(tableName2)
    // 计算auc
    val aucData = getAuc(tableName2, conversionGoal, version, date, hour, spark)
    val resultDF = aucData
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))
//    test.ocpc_check_auc_data20190104_bak
    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_userid_auc_daily")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_userid_auc_daily")
  }

  def getData(conversionGoal: String, version: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史区间: score数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition1 = s"`date`='$date1'"

    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    userid,
         |    ext['exp_cvr'].int_value as score
         |from dl_cpc.cpc_union_log
         |where $selectCondition1
         |and isclick = 1
         |and ext['exp_ctr'].int_value is not null
         |and media_appsid  in ("80000001", "80000002")
         |and ext['antispam'].int_value = 0
         |and ideaid > 0 and adsrc = 1
         |and ext_int['dsp_adnum_by_src_1'] > 1
         |and userid > 0
         |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark.sql(sqlRequest)

    // 取历史区间: cvr数据
    calendar.add(Calendar.DATE, 2)
    val yesterday1 = calendar.getTime
    val date2 = dateConverter.format(yesterday1)
    val selectCondition2 = s"`date` between '$date1' and '$date2'"
    // 根据conversionGoal选择cv的sql脚本
    var sqlRequest2 = ""
    if (conversionGoal == "1") {
      // cvr1数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  label2 as label
           |FROM
           |  dl_cpc.ml_cvr_feature_v1
           |WHERE
           |  $selectCondition2
           |AND
           |  label2=1
           |AND
           |  label_type!=12
           |GROUP BY searchid, label2
       """.stripMargin
    } else if (conversionGoal == "2") {
      // cvr2数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  label
           |FROM
           |  dl_cpc.ml_cvr_feature_v2
           |WHERE
           |  $selectCondition2
           |AND
           |  label=1
           |GROUP BY searchid, label
       """.stripMargin
    } else {
      // cvr3数据
      sqlRequest2 =
        s"""
           |SELECT
           |  searchid,
           |  1 as label
           |FROM
           |  dl_cpc.site_form_unionlog
           |WHERE
           |  $selectCondition2
           |AND
           |  ideaid>0
       """.stripMargin
    }
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
    import spark.implicits._
    //获取模型标签

//    val aucGaucBuffer = ListBuffer[AucGauc.AucGauc]()
    val data = spark
      .table(tableName)
      .where(s"`date`='$date' and conversion_goal='$conversionGoal' and version='$version'")


    val aucList = new mutable.ListBuffer[(String, Double)]()
    val useridList = data.select("userid").distinct().cache()
    val useridCnt = useridList.count()
    println(s"################ count of userid list: $useridCnt ################")

    //按userid遍历
    var cnt = 0
    for (row <- useridList.collect()) {
      val userid = row.getAs[Int]("userid").toString
      println(s"############### userid=$userid, cnt=$cnt ################")
      cnt += 1
      val userData = data.filter(s"userid=$userid")
      val scoreAndLabel = userData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))
        .cache()
      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        aucList.append((userid, aucROC))

      }
      scoreAndLabel.unpersist()
    }

    useridList.unpersist()
    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("userid", "auc")

    resultDF
  }
}