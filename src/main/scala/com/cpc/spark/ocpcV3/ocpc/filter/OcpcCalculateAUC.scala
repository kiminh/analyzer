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
    val spark = SparkSession
      .builder()
      .appName(s"ocpc ideaid auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val data = getData(date, hour, spark)

    // 计算auc
    // cvr1
    val auc1Data = getAuc(data, 1, date, hour, spark)
    // cvr2
    val auc2Data = getAuc(data, 2, date, hour, spark)
    // cvr3
    val auc3Data = getAuc(data, 3, date, hour, spark)

    // 合并数据
    val aucData = auc1Data.union(auc2Data).union(auc3Data)
    aucData.show(10)
    aucData.write.mode("overwrite").saveAsTable("test.ocpc_qtt_auc_ideaid20190103")
  }

  def getData(date: String, hour: String, spark: SparkSession) = {
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
         |    ideaid,
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
    calendar.add(Calendar.DATE, 3)
    val yesterday1 = calendar.getTime
    val date2 = dateConverter.format(yesterday1)
    val selectCondition2 = s"`date` between '$date1' and '$date2'"
    // cvr1数据
    val sqlRequest1 =
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
         |GROUP BY searchid, label2
       """.stripMargin
    println(sqlRequest1)
    val cvr1Data = spark.sql(sqlRequest1)

    // cvr2数据
    val sqlRequest2 =
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
    println(sqlRequest2)
    val cvr2Data = spark.sql(sqlRequest2)

    // cvr3数据
    val sqlRequest3 =
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
    println(sqlRequest3)
    val cvr3Data = spark.sql(sqlRequest3).distinct()


    // 关联数据
    val result1 = scoreData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .withColumn("conversion_goal", lit(1))
      .select("searchid", "ideaid", "score", "label", "conversion_goal")
    val result2 = scoreData
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .withColumn("conversion_goal", lit(2))
      .select("searchid", "ideaid", "score", "label", "conversion_goal")
    val result3 = scoreData
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .withColumn("conversion_goal", lit(3))
      .select("searchid", "ideaid", "score", "label", "conversion_goal")

    // 合并数据
    val resultDF = result1
      .union(result2)
      .union(result3)
      .na.fill(0, Seq("label"))
    resultDF.show(10)
    resultDF
  }

  def getAuc(rawData: DataFrame, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._
    //获取模型标签

//    val aucGaucBuffer = ListBuffer[AucGauc.AucGauc]()
    val data = rawData.filter(s"conversion_goal=$conversionGoal")
    val aucList = new mutable.ListBuffer[(String, Double)]()
    val ideaidList = data.select("ideaid").distinct()
    val ideaidCNT = ideaidList.count()
    println(s"################ count of ideaid list: $ideaidCNT ################")

    //按ideaid遍历
    var cnt = 0
    for (row <- ideaidList.collect()) {
      val ideaid = row.getAs[String]("ideaid")
      if (cnt % 500 == 0) {
        println(s"############### ideaid=$ideaid ################")
      }
      cnt += 1
      val ideaidData = data.filter(s"ideaid=$ideaid")
      val scoreAndLabel = ideaidData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))
      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        aucList.append((ideaid, aucROC))

      }
    }
    val resultDF = spark
      .createDataFrame(aucList)
      .toDF("ideaid", "auc")
      .withColumn("conversion_goal", lit(conversionGoal))

    resultDF
  }
}