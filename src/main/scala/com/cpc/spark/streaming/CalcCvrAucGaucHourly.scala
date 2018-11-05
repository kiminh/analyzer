package com.cpc.spark.streaming
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
/**
  * @author Jinbao
  * @date 2018/11/5 15:04
  */

object CalcCvrAucGaucHourly {


    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"CalcCvrAucGaucHourly date = $date , hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._
        val model = "cvrmodel"

        val unionSql =
            s"""
               |select searchid, exptags, uid,
               |ext['exp_cvr'].int_value as score,
               |adslot_type
               |from dl_cpc.cpc_union_log
               |where `date` = '$date' and hour = '$hour'
               |and adslot_type in (1,2,3) and isshow = 1
               |and ext['exp_cvr'].int_value is not null
               |and media_appsid  in ('80000001', '80000002')
               |and ext['antispam'].int_value = 0
               |and ideaid > 0 and adsrc = 1
               |and ext_int['dsp_adnum_by_src_1'] > 1
               |and userid > 0
               |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
             """.stripMargin
        val union = spark.sql(unionSql)
          .filter("exptags like '%cvrmodel%'")
          .withColumn("exptag",getExptag(col("exptags")))
          .drop("exptags")

        val cvrSql =
            s"""
               |select searchid,label
               |from dl_cpc.ml_cvr_feature_v1
               |where `date` = '$date' and hour = '$hour'
             """.stripMargin

        val cvr = spark.sql(cvrSql)

        val unionJoincvr = union.join(cvr,Seq("searchid")).cache()

        unionJoincvr.show(2)

        val exptag = unionJoincvr.select("exptag")
          .distinct()
          .collect()
          .map(x => x.getAs[String]("exptag"))
        println(exptag.mkString(" "))
        val aucGaucBuffer = ListBuffer[AucGauc]()

        for (adslot_type <- 1 to 3) {
            for (exp <- exptag) {
                val unionJoincvrFilter = unionJoincvr
                  .filter(s"adslot_type = $adslot_type and exptag = $exp")
                  .coalesce(400)
                  .cache()
                val ScoreAndLabel = unionJoincvrFilter
                  .select($"score",$"label")
                  .rdd
                  .map(x => (x.getAs[Double]("score"), x.getAs[Double]("label")))

                val ScoreAndLabelNum = ScoreAndLabel.count()
                if (ScoreAndLabelNum > 0) {
                    val metrics = new BinaryClassificationMetrics(ScoreAndLabel)
                    val aucROC = metrics.areaUnderROC

                    val aucAndSum = unionJoincvrFilter
                      .select($"uid",$"score",$"label")
                      .rdd
                      .map(x => (x.getAs[String]("uid"),
                        (x.getAs[Double]("score"), x.getAs[Double]("label"))))
                      .combineByKey(
                          x => List(x),
                          (x: List[(Double, Double)], y: (Double, Double)) => y :: x,
                          (x: List[(Double, Double)], y: List[(Double, Double)]) => x ::: y
                      )
                      .mapValues(x => {
                          val label = x.map(x => x._2)
                          val max = label.max.toInt
                          val pos = Array.fill(max)(0)
                          val neg = Array.fill(max)(0)
                          val n = label.sum //正样本数
                          val m = x.length - n  //负样本数

                          for ((s,l) <- x){
                              if (s.toInt == 0) neg(l.toInt) += 1
                              else pos(l.toInt) += 1
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

                    aucGaucBuffer += AucGauc(auc = aucROC,
                        gauc = gaucROC,
                        adslot = adslot_type,
                        modeltype = s"%$exp%",
                        date = date,
                        hour = hour)
                }
                unionJoincvrFilter.unpersist()


            }
        }

        val aucGauc = aucGaucBuffer.toList.toDF()

        aucGauc.write.mode("overwrite").insertInto("dl_cpc.cpc_cvr_auc_gauc_hourly_log1")
    }

    def getExptag = udf((exptags:String) => {
        val s = exptags.toString.split(",")
        var exptag = ""
        for (str <- s) {
            if (str.contains("cvrmodel")) {
                val i = str.indexOf("=")
                exptag = str.substring(i + 1).trim
            }
        }
        exptag
    })

    case class AucGauc(var auc:Double = 0,
                       var gauc:Double = 0,
                       var adslot:Int = 0,
                       var modeltype:String = "",
                       var date:String = "",
                       var hour:String = "")

}
