package com.cpc.spark.qukan.userprofile

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vector

/**
  * 使用lr模型预测人群包tag数据推到redis
  *
  * @author zhj
  * @version 1.0
  *          2018-08-07
  */
object Crowd2Redis_lr {
  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      System.err.println(
        """
          | crowd bag data predicting : <lr_model_path> <data_path>
          |           <up_threshold>  <down_threshold>
          |            <u_tag> <d_tag>
        """.stripMargin)
    }
    val model_path = args(0)
    val data_path = args(1)
    val u_threshold = args(2).toDouble //0.7
    val d_threshold = args(3).toDouble //0.3
    val u_tag = args(4).toInt
    val d_tag = args(5).toInt

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val data = spark.read.parquet(data_path)
      .map { x => (x.getAs[String]("uid"), x.getAs[Vector]("features")) }
      .toDF("uid", "features")

    val model = LogisticRegressionModel.load(model_path)

    val predictions = model.transform(data).map{x=>
      (x.getAs[String]("uid"),x.getAs[Vector]("probability")(1))
    }.toDF("uid","prediction")
    if (u_threshold > 0 && u_tag > 0) {
      val uids = predictions.filter(s"prediction>$u_threshold")
        .map(x => (x.getAs[String]("uid"), u_tag, true))
      SetUserProfileTag.setUserProfileTag(uids.rdd)
    }
    if (d_threshold > 0 && d_tag > 0) {
      val uids = predictions.filter(s"prediction<$d_threshold")
        .map(x => (x.getAs[String]("uid"), d_tag, true))
      SetUserProfileTag.setUserProfileTag(uids.rdd)
    }
  }
}
