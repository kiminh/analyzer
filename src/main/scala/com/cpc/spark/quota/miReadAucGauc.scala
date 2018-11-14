package com.cpc.spark.quota
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import scala.collection.mutable.ListBuffer
/**
  * @author Jinbao
  * @date 2018/11/13 15:49
  */
object miReadAucGauc {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"miReadAucGauc date = $date , hour = $hour")
          .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val unionSql =
            s"""
               |select exptags,uid,
               |isclick as label,
               |ext['exp_ctr'].int_value as score
               |from dl_cpc.cpc_union_log
               |where `date` = '$date' and hour = '$hour'
               |and isshow = 1
               |and ext['exp_ctr'].int_value is not null
               |and media_appsid  in ('80001098', '80001292')
               |and ext['antispam'].int_value = 0
               |and ideaid > 0 and adsrc = 1
               |and ext_int['dsp_adnum_by_src_1'] > 1
               |and userid > 0
               |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
             """.stripMargin
        val union = spark.sql(unionSql)
          .filter("exptags like '%ctrmodel%'")
          .withColumn("exptag",getExptag(col("exptags")))
          .drop("exptags")
          .cache()

        val exptag = union.select("exptag")
          .distinct()
          .collect()
          .map(x => x.getAs[String]("exptag"))

        val aucGaucBuffer = ListBuffer[AucGauc]()

        for (exp <- exptag) {
            println(s"******************$exp******************")
            val unionSingleExp = union.filter(s"exptag = '$exp'")
              .coalesce(100)
              .cache()
            val scoreAndLable = unionSingleExp.select($"score",$"label")
              .rdd
              .map(x => (x.getAs[Int]("score").toDouble, x.getAs[Int]("label").toDouble))
            val scoreAndLabelNum = scoreAndLable.count()
            println("scoreAndLable 's count is " + scoreAndLabelNum)
            if (scoreAndLabelNum > 0) {
                val metrics = new BinaryClassificationMetrics(scoreAndLable)
                val aucROC = metrics.areaUnderROC
                val aucAndSum = unionSingleExp
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

                println(s"aucROC = $aucROC , gaucROC = $gaucROC , modeltype = %$exp%")
                aucGaucBuffer += AucGauc(auc = aucROC,
                    gauc = gaucROC,
                    adslot = 0,
                    modeltype = s"%$exp%",
                    date = date,
                    hour = hour)
            }
            unionSingleExp.unpersist()
        }

        val aucGauc = aucGaucBuffer.toList.toDF()

        aucGauc.coalesce(1).write.mode("overwrite").insertInto("dl_cpc.miReadAucGauc")
        println("insert into dl_cpc.miReadAucGauc success!")
        spark.stop()
    }
    def getExptag = udf((exptags:String) => {
        val s = exptags.toString.split(",")
        var exptag = ""
        for (str <- s) {
            if (str.contains("ctrmodel")) {
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
