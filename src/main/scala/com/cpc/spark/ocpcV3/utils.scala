package com.cpc.spark.ocpcV3

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer
/**
  * @author Jinbao
  * @date 2018/12/22 16:35
  */
object utils {
  case class gauc(var name:String = "",
                  var auc:Double = 0.0,
                  var sum:Double = 0.0)
  def getAuc(spark:SparkSession, data:DataFrame): Double = {
    import spark.implicits._
    val scoreAndLable = data.select($"score",$"label")
      .rdd
      .map(x => (x.getAs[Int]("score").toDouble,
        x.getAs[Int]("label").toDouble))
    val metrics = new BinaryClassificationMetrics(scoreAndLable)
    val aucROC = metrics.areaUnderROC
    aucROC
  }
  def getGauc(spark:SparkSession, data:DataFrame, g:String = "uid"):DataFrame = {
    import spark.implicits._
    val aucAndSum = data
      .select(g,"score","label")
      .rdd
      .map(x => (x.getAs[String](g),
        (x.getAs[Int]("score"), x.getAs[Int]("label"))))
      .combineByKey(
        x => List(x),
        (x: List[(Int, Int)], y: (Int, Int)) => y :: x,
        (x: List[(Int, Int)], y: List[(Int, Int)]) => x ::: y
      )
      .mapValues(x => {
        val label = x.map(x => x._2)
        val max = x.map(x => x._1).max + 2
        val pos = Array.fill(max)(0)
        val neg = Array.fill(max)(0)
        val n = label.sum //正样本数
        val m = x.length - n  //负样本数

        for ((s,l) <- x){
          if (l == 0) neg(s) += 1
          else pos(s) += 1
        }

        var negSum = 0
        var auc: Double = 0
        for (i <- 0 to max - 1) {
          auc += 1.0 * pos(i) * negSum + pos(i) * neg(i) * 0.5
          negSum += neg(i)
        }
        val result = if (m <= 0 || n <= 0) (-1.0, 0.0 + m + n) else (auc / (1.0 * m * n), 0.0 + m + n)
        result
      }).map(x => (x._1,x._2._1,x._2._2))
      .map(x => gauc(name = x._1, auc = x._2, sum = x._3))
      .toDF()
    aucAndSum
  }
}

