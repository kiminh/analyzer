package com.cpc.spark.ocpcV3.ocpc.filter

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
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
  }

//  def getAucGauc(spark: SparkSession, data: DataFrame, adslotTypes:List[Int] = List(-1),
//                 date:String, hour:String): List[AucGauc.AucGauc] = {
//    import spark.implicits._
//    //获取模型标签
//    val exptag = data.select("exptag")
//      .distinct()
//      .collect()
//      .map(x => x.getAs[String]("exptag"))
//
//    val aucGaucBuffer = ListBuffer[AucGauc.AucGauc]()
//
//    //按栏位遍历
//    for (adslot_type <- adslotTypes) {
//      val dataFilterDaslotType =
//        if (adslot_type == -1)
//          data
//        else
//          data.filter(s"adslot_type = $adslot_type")
//      dataFilterDaslotType.cache()
//      //按模型遍历
//      for (exp <- exptag) {
//        val dataFilterExp = dataFilterDaslotType.filter(s"exptag = '$exp'")
//        val scoreAndLable = dataFilterExp.select($"score",$"label")
//          .rdd
//          .map(x => (x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))
//        val scoreAndLabelNum = scoreAndLable.count()
//        if (scoreAndLabelNum > 0) {
//          val metrics = new BinaryClassificationMetrics(scoreAndLable)
//          val aucROC = metrics.areaUnderROC
//
//          val aucAndSum = dataFilterExp
//            .select($"uid",$"score",$"label")
//            .rdd
//            .map(x => (x.getAs[String]("uid"),
//              (x.getAs[Int]("score"), x.getAs[Int]("label"))))
//            .combineByKey(
//              x => List(x),
//              (x: List[(Int, Int)], y: (Int, Int)) => y :: x,
//              (x: List[(Int, Int)], y: List[(Int, Int)]) => x ::: y
//            )
//            .mapValues(x => {
//              val label = x.map(x => x._2)
//              val max = x.map(x => x._1).max + 2
//              val pos = Array.fill(max)(0)
//              val neg = Array.fill(max)(0)
//              val n = label.sum //正样本数
//              val m = x.length - n  //负样本数
//
//              for ((s,l) <- x){
//                if (l == 0) neg(s) += 1
//                else pos(s) += 1
//              }
//
//              var negSum = 0
//              var auc: Double = 0
//              for (i <- 0 to max - 1) {
//                auc += 1.0 * pos(i) * negSum + pos(i) * neg(i) * 0.5
//                negSum += neg(i)
//              }
//              val result = if (m <= 0 || n <= 0) (0.0, 0.0) else (auc / (1.0 * m * n), 0.0 + m + n)
//              result
//            })
//            .map(x => x._2)
//
//          //计算分子
//          val auc = aucAndSum.map(x => x._1 * x._2).reduce((x, y) => x+y)
//          //计算分母
//          val sum = aucAndSum.map(x => x._2).reduce((x, y) => x+y)
//
//          val gaucROC = if (sum > 1e-6) auc / sum else 0.0
//
//          aucGaucBuffer += AucGauc.AucGauc(auc = aucROC,
//            gauc = gaucROC,
//            adslot_type = adslot_type,
//            model = s"%$exp%",
//            date = date,
//            hour = hour)
//
//        }
//      }
//      dataFilterDaslotType.unpersist()
//    }
//    aucGaucBuffer.toList
//  }
}