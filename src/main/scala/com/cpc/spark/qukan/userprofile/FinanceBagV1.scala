package com.cpc.spark.qukan.userprofile


import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector

/**
  * 推到redis记录：2018-07-13
  * created by zhj
  */
object FinanceBagV1 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println(
        """
          | crowd bag data predicting : <category> <tag>
        """.stripMargin)
    }
    val category = args(0)
    val tag = args(1).toInt

    val version = "v1"

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val data = spark.read.parquet(s"/home/cpc/zhj/crowd_bag/$category/$version/data/predictdata")
      .map { x => (x.getAs[String]("uid"), x.getAs[Vector]("features")) }
      .toDF("uid", "features")

    val model = XGBoostModel.load(s"/home/cpc/zhj/crowd_bag/$category/$version/model/xgboost")

    val predictions = model.transform(data)
    val uids = predictions.filter("prediction>0.15")
      .map(x => (x.getAs[String]("uid"), tag, true))
    SetUserProfileTag.setUserProfileTag(uids.rdd)
  }
}
