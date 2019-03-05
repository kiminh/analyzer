package com.cpc.spark.common

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, expr, row_number}
import scala.collection.mutable.ListBuffer
/**
  * @author Jinbao
  * @date 2018/12/22 16:35
  */
object CalcMetrics {
    case class gauc(var name:String = "",
                    var auc:Double = 0.0,
                    var sum:Double = 0.0)
    def getAuc(
                spark : SparkSession,
                data : DataFrame,
                label_name : String
              ): Double = {
        val scoreAndLabel = data.select(
            col("score"),
            col(label_name))
          .rdd
          .map( x => {
              (
                x.getAs[Int]("score").toDouble,
                x.getAs[Int](label_name).toDouble
              )
          })

        val metrics = new BinaryClassificationMetrics(scoreAndLabel)
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
    def getAucGauc(spark: SparkSession, data: DataFrame, adslotTypes:List[Int] = List(-1),
                   date:String, hour:String): List[AucGauc.AucGauc] = {
        import spark.implicits._
        //获取模型标签
        val exptag = data.select("exptag")
          .distinct()
          .collect()
          .map(x => x.getAs[String]("exptag"))

        val aucGaucBuffer = ListBuffer[AucGauc.AucGauc]()

        //按栏位遍历
        for (adslot_type <- adslotTypes) {
            val dataFilterDaslotType =
                if (adslot_type == -1)
                    data
                else
                    data.filter(s"adslot_type = $adslot_type")
            dataFilterDaslotType.cache()
            //按模型遍历
            for (exp <- exptag) {
                val dataFilterExp = dataFilterDaslotType.filter(s"exptag = '$exp'")
                val scoreAndLable = dataFilterExp.select($"score",$"label")
                  .rdd
                  .map(x => (x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))
                val scoreAndLabelNum = scoreAndLable.count()
                if (scoreAndLabelNum > 0) {
                    val metrics = new BinaryClassificationMetrics(scoreAndLable)
                    val aucROC = metrics.areaUnderROC

                    val aucAndSum = dataFilterExp
                      .select($"uid",$"score",$"label")
                      .rdd
                      .map(x => (x.getAs[String]("uid"),
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
                          val result = if (m <= 0 || n <= 0) (0.0, 0.0) else (auc / (1.0 * m * n), 0.0 + m + n)
                          result
                      })
                      .map(x => x._2)

                    //计算分子
                    val auc = aucAndSum.map(x => x._1 * x._2).reduce((x, y) => x+y)
                    //计算分母
                    val sum = aucAndSum.map(x => x._2).reduce((x, y) => x+y)

                    val gaucROC = if (sum > 1e-6) auc / sum else 0.0

                    aucGaucBuffer += AucGauc.AucGauc(auc = aucROC,
                            gauc = gaucROC,
                            adslot_type = adslot_type,
                            model = s"%$exp%",
                            date = date,
                            hour = hour)

                }
            }
            dataFilterDaslotType.unpersist()
        }
        aucGaucBuffer.toList
    }

    object AucGauc {
        case class AucGauc(var auc:Double = 0,
                           var gauc:Double = 0,
                           var adslot_type:Int = 0,
                           var model:String = "",
                           var date:String = "",
                           var hour:String = "")
    }
}
