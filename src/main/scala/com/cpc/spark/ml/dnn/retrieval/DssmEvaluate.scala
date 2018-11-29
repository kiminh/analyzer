package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.retrieval.UserEmbeddingToRedis.{hdfsDir => userHDFSDir}
import com.cpc.spark.ml.dnn.retrieval.AdEmbeddingToFile.{hdfsDir => adHDFSDir}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * author: huazhenhao
  * date: 11/28/18
  */
object DssmEvaluate {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dssm-ad-eval").enableHiveSupport().getOrCreate()
    import spark.implicits._

    val date = args(0)

    val labels = getLabels(date, spark)
    val userEmbedding = getUserEmbedding(date, spark)
    val adEmbeding = getAdEmbedding(date, spark)
    val df = labels.join(userEmbedding, Seq("uid"), "left")
      .join(adEmbeding, Seq("ideaid"), "left")
      .rdd.map(x => {
      val uid = x.getAs[String]("uid")
      val ideaid = x.getAs[Number]("ideaid").intValue()
      val userEmbedding = x.getAs[mutable.WrappedArray[Double]]("user_embedding")
      val adEmbedding = x.getAs[mutable.WrappedArray[Double]]("ad_embedding")
      val userisNull = if (userEmbedding == null) 1 else 0
      val adIsNull = if (adEmbedding == null) 1 else 0
      val clickCount = x.getAs[Long]("click")
      var score = 0.0
      var userEmbeddingStr = ""
      var adEmbeddingStr = ""
      if (userEmbedding != null && adEmbedding != null) {
        var sum = 0.0
        for (i <- userEmbedding.indices) {
          sum = sum + userEmbedding(i) * adEmbedding(i)
        }
        score = sum
        userEmbeddingStr = userEmbedding.mkString(",")
        adEmbeddingStr = adEmbedding.mkString(",")
      }
      (uid, ideaid, clickCount, score, userisNull, adIsNull, userEmbeddingStr, adEmbeddingStr, date)
    }).toDF("uid", "ideaid", "clickCount", "score", "userNull", "adNull",
      "userEmbeddingStr", "adEmbeddingStr","dt")

    val metrics = new BinaryClassificationMetrics(
      df.filter(row => row.getAs[Int]("userNull") == 0 && row.getAs[Int]("adNull") == 0)
        .rdd.map(row =>
        (row.getAs[Double]("score"), if (row.getAs[Long]("clickCount") > 0) 1 else 0)))
    println(s"auc: ${metrics.areaUnderROC()}")

    df.write.mode("overwrite")
      .insertInto("dl_cpc.dssm_eval_raw")
  }

  def getLabels(date: String, spark: SparkSession): DataFrame = {
    val sql =
      s"""
         |select
         |  uid as uid,
         |  ideaid as ideaid,
         |  sum(isclick) as click
         |from dl_cpc.cpc_union_log where `date` = '$date' and hour = '20'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and length(uid) > 1
         |group by uid, ideaid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")
    spark.sql(sql)
  }

  def getUserEmbedding(date: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val data = spark.read.parquet(userHDFSDir + date)
    data.rdd.map(
      x => {
        val uid = x.getAs[Array[Byte]](64).map(_.toChar).mkString
        val embedding = new Array[Double](64)
        for (i <- 0 to 63) {
           embedding(i) = x.getAs[Number](i).doubleValue()
        }
        (uid, embedding)
      }
    ).toDF("uid", "user_embedding")
  }

  def getAdEmbedding(date: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val data = spark.read.parquet(adHDFSDir + date)
    data.rdd.map(
      row => {
        val ideaid = row.getAs[Array[Byte]](64).map(_.toChar).mkString.toInt
        val embedding = new Array[Double](64)

        for (i <- 0 to 63) {
          embedding(i) = row.getAs[Number](i).doubleValue()
        }
        (ideaid, embedding)
      }
    ).toDF("ideaid", "ad_embedding")
  }
}
