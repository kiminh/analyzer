package com.cpc.spark.ocpcV3.ocpc.toolkit

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object calculateAUC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(s"model auc")
      .enableHiveSupport().getOrCreate()

    val data = spark
      .table("test.check_cvr_model20190108")


    val aucList = new mutable.ListBuffer[(String, Double)]()
    val model = data.select("model").distinct().cache()

    for (row <- model.collect()) {
      val modelName = row.getAs[String]("model")
      val singleData = data.filter(s"model='$modelName'")

      val scoreAndLabel = singleData
        .select("score", "label")
        .rdd
        .map(x=>(x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))

      val scoreAndLabelNum = scoreAndLabel.count()
      if (scoreAndLabelNum > 0) {
        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
        val aucROC = metrics.areaUnderROC
        aucList.append((modelName, aucROC))

      }

      model.unpersist()
      val resultDF = spark
        .createDataFrame(aucList)
        .toDF("model", "auc")

      resultDF.show(10)
    }
  }
}