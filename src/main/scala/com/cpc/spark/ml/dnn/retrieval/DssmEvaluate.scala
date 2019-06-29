package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.retrieval.UserEmbeddingToRedis.{hdfsDir => userHDFSDir}
import com.cpc.spark.ml.dnn.retrieval.AdEmbeddingToFile.{hdfsDir => adHDFSDir}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * author: huazhenhao
  * date: 11/28/18
  */
object DssmEvaluate {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("dssm-ad-eval").enableHiveSupport().getOrCreate()

    val yesterday = args(0)
    val twodayago = args(1)


    println("=======yesterday eval start========")
    eval(spark, yesterday, yesterday, "left",1)
    println("=======yesterday eval end========")

    println("=======twodayago eval start========")
    eval(spark, yesterday, twodayago, "inner",2)
    println("=======twodayago eval end========")

  }

  def eval(spark: SparkSession, yesterday: String, testday:String, joinType:String, dateago:Int): Unit = {
    import spark.implicits._
    val labels = getLabels(yesterday, spark)
    val labelCount = labels.count()

    val userEmbedding = getUserEmbedding(testday, spark)
    val userEmbeddingCount = userEmbedding.count()
    println("user embedding data = " + userEmbeddingCount)

    val adEmbeding = getAdEmbedding(testday, spark)
    val adEmbeddingCount = adEmbeding.count()
    println("ad embedding data = " + adEmbeddingCount)

    var df = labels.join(userEmbedding, Seq("uid"), joinType)
      .join(adEmbeding, Seq("adid"), joinType)
      .rdd.map(x => {
      val uid = x.getAs[String]("uid")
      val adid = x.getAs[String]("adid")
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
      (uid, adid, clickCount, score, userisNull, adIsNull, userEmbeddingStr, adEmbeddingStr)
    }).toDF("uid", "adid", "clickcount", "score", "usernull", "adnull",
      "userembedding", "adembedding").repartition(1000).cache()

    println("labels count = " + labelCount)
    println("user embedding data = " + userEmbeddingCount)
    println("ad embedding data = " + adEmbeddingCount)
    val dfCount = df.count()
    println("df count = " + dfCount)

    val metrics = new BinaryClassificationMetrics(
      df.filter(row => row.getAs[Int]("usernull") == 0 && row.getAs[Int]("adnull") == 0)
        .rdd.map(row =>
        (row.getAs[Double]("score"), if (row.getAs[Long]("clickcount") > 0) 1 else 0)))
    val auc=metrics.areaUnderROC()
    println(s"auc: ${auc}")

    println("df user embedding distinct count = " + df.select($"userembedding").distinct().count())
    df.createOrReplaceTempView("tmp_table")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.dssm_eval_raw_new partition (`dt`='$yesterday', dateago=$dateago)
         |select * from tmp_table
       """.stripMargin)

    val sigmoidUdf = udf((x: Double) => {
      1.0 / (1.0 + Math.exp(-x))
    })
    df = df.withColumn("scoresigmod", sigmoidUdf(df("score")))
    val isClickUdf = udf((x: Long) => {
      if (x > 0) 1 else 0
    })
    df = df.withColumn("isclick", isClickUdf(df("clickcount")))

    df.show(10)

    df.createOrReplaceTempView("tmp_table2")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.dssm_eval_new partition (`dt`='$yesterday', dateago=$dateago)
         |select mean(scoresigmod) as scoresigmod,mean(isclick) as isclick,${auc} as auc from tmp_table2
       """.stripMargin)

    val meanDF = spark.sql(
      s"""
         |select * from dl_cpc.dssm_eval_new where `dt`='$yesterday' and dateago=$dateago
       """.stripMargin)
    meanDF.show(10)


    df.unpersist()
    userEmbedding.unpersist()
    adEmbeding.unpersist()
    meanDF.unpersist()
  }

  def getLabels(date: String, spark: SparkSession): DataFrame = {
    val sql =
      s"""
         |select
         |  uid as uid,
         |  concat(cast(ideaid as string),'_',cast(unitid as string)) as adid,
         |  sum(isclick) as click
         |from dl_cpc.cpc_basedata_union_events where `day` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and length(uid) > 1
         |group by uid, concat(cast(ideaid as string),'_',cast(unitid as string))
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")
    spark.sql(sql)
  }

  def getUserEmbedding(date: String, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val data = spark.read.parquet(userHDFSDir + date)
    data.show(10)
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
    data.show(10)
    data.rdd.map(
      row => {
        val adid = row.getAs[Array[Byte]](0).map(_.toChar).mkString
        val embedding = new Array[Double](64)

        for (i <- 1 to 64) {
          embedding(i-1) = row.getAs[Number](i).doubleValue()
        }
        (adid, embedding)
      }
    ).toDF("adid", "ad_embedding")
  }
}
