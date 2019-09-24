package com.cpc.spark.ml.calibration

import java.io.FileInputStream

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.CalibrationConfig
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions.{udf, _}

/**
  * author: huazhenhao
  * date: 9/19/18
  */
object CpmAuc {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("cpm auc")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sql =
      s"""
         | select cpm,cast(isclick as int) as isclick,cast(raw_ctr as int) raw_ctr,ctr_model_name
         |    from dl_cpc.slim_union_log a
         |    join
         |    (
         |    select ideaid, sum(if(isclick = 1,price,0))/sum(isshow)*10 cpm
         |    from dl_cpc.slim_union_log
         |    where dt = '2019-09-15'
         |    and media_appsid in ('80000001','80000002')
         |    and isshow = 1
         |    and adslot_type = 1
         |    and adtype != 15
         |    and adsrc in (1,28)
         |    group by ideaid
         |    ) b
         |    on a.ideaid = b.ideaid
         |    where dt = '2019-09-15'
         |    and media_appsid in ('80000001','80000002')
         |    and isshow = 1
         |    and ctr_model_name = 'qtt-list-dnn-rawid-v4rockefeller-up'
         |    and adslot_type = 1
         |    and adtype != 15
         |    and adsrc in (1,28)
         |""".stripMargin

    println(sql)

    val basedata = spark.sql(sql)
      .withColumn("score",col("raw_ctr"))
      .withColumn("label",col("isclick"))
      .repartition(1000)

    val result = getCpmAuc(spark,basedata)
    result.show(10)
    result.write.mode("overwrite").saveAsTable("dl_cpc.wy_test00")
    println("job success")

  }
  case class LabeledPred(predict: Double, label: Int)

  def getCpmAuc(spark:SparkSession, data:DataFrame, g:String = "ctr_model_name"):DataFrame = {
    import spark.implicits._
    val aucAndSum = data
      .select("score","label","cpm",g)
      .rdd
      .map(x => (x.getAs[String](g),
        (x.getAs[Int]("score"), x.getAs[Int]("label"),x.getAs[Double]("cpm"))))
      .combineByKey(
        x => List(x),
        (x: List[(Int, Int, Double)], y: (Int, Int, Double)) => y :: x,
        (x: List[(Int, Int, Double)], y: List[(Int, Int, Double)]) => x ::: y
      )
      .mapValues(x => {
        val label = x.map(x => x._2)
        val max = x.map(x => x._1).max + 2
        val cpm = x.map(x => x._2)
        val pos = Array.fill(max)(0.0)
        val neg = Array.fill(max)(0.0)
//        val n = label.sum //正样本数
//        val m = x.length - n  //负样本数

        for ((s,l,c) <- x){
          if (l == 0) neg(s) += c
          else pos(s) += c
        }
        val n = pos.sum
        val m = neg.sum

        var negSum = 0.0
        var auc: Double = 0
        for (i <- 0 to max - 1 ) {
          auc += 1.0 * pos(i) * negSum + pos(i) * neg(i) * 0.5
          negSum += neg(i)
        }
        val result = if (m <= 0.0 || n <= 0.0) (-1.0, 0.0 + m + n) else (auc / (1.0 * m * n), 0.0 + m + n)
        result
      }).map(x => (x._1,x._2._1,x._2._2))
      .map(x => CpmAuc(model = x._1, auc = x._2, sum = x._3))
      .toDF()
    aucAndSum
  }

  case class CpmAuc(
                     var model: String="",
                     var auc: Double=0,
                     var sum: Double=0
                   )

  def auc(points: Seq[LabeledPred]) = {
    val posNum = points.count(_.label > 0)
    val negNum = points.length - posNum

    if (posNum == 0 || negNum == 0) {
      println("Error: Lables of all samples are the same.")
      0.0
    } else {
      val sorted = points.sortBy(_.predict)

      var negSum = 0
      var posGTNeg = 0
      for (p <- sorted) {
        if (p.label > 0) {
          posGTNeg = posGTNeg + negSum
        } else {
          negSum = negSum + 1
        }
      }

      posGTNeg.toDouble/(posNum * negNum).toDouble
    }
  }
}
