package com.cpc.spark.ml.novel.auc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.ml.novel.auc.CalcMetrics
import scala.collection.mutable.ListBuffer

/**
 * @author Jinbao
 * @date 2018-10-18
 */
object NovelCvrAucGaucbyadclass {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)

        val spark = SparkSession.builder()
          .appName(s"NovelCvrAucGaucbyadclass date = $date , hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._
        val model = "cvrmodel"

        val unionSql =
            s"""
               |select searchid, exptags, uid, ext['adclass'].int_value as adclass, userid, adslotid,
               |ext['exp_cvr'].int_value as score,
               |adslot_type
               |from dl_cpc.cpc_novel_union_log
               |where `date` = '$date'
               |and isshow = 1
               |and ext['exp_cvr'].int_value is not null
               |and media_appsid  in ('80001098', '80001292')
               |and ext['antispam'].int_value = 0
               |and ideaid > 0 and adsrc = 1
               |and ext_int['dsp_adnum_by_src_1'] > 1
               |and userid > 0
               |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
             """.stripMargin
        println(unionSql)
        val union = spark.sql(unionSql)
          .filter("exptags like '%cvrmodel%'")
          .withColumn("exptag",getExptag(col("exptags")))
          .drop("exptags")

        val cvrSql =
            s"""
               |select searchid,label2 as label
               |from dl_cpc.ml_cvr_feature_v1
               |where `date` = '$date'
             """.stripMargin
        println(cvrSql)
        val cvr = spark.sql(cvrSql)


        val unionJoincvr = union.join(cvr,Seq("searchid")).cache()

        unionJoincvr.show(2)

        val exptag = unionJoincvr.select("exptag").distinct().collect().map(x => x.getAs[String]("exptag"))
        val adslotid = unionJoincvr.select("adslotid").distinct().collect().map(x => x.getAs[String]("adslotid"))
        val adclass = unionJoincvr.select("adclass").distinct().collect().map(x => x.getAs[Int]("adclass"))
        val userid = unionJoincvr.select("userid").distinct().collect().map(x => x.getAs[Int]("userid"))
        //println(exptag.mkString(" "))
        val aucGaucBuffer = ListBuffer[AucGauc]()

        for (x <- adslotid) {
            for (exp <- exptag) {
                val unionJoincvrFilter = unionJoincvr
                  .filter(s"exptag = '$exp' and adslotid = $x")
                  .coalesce(400)
                  .cache()
                if (unionJoincvrFilter.count() > 0) {
                    val aucROC = CalcMetrics.getAuc(spark, unionJoincvrFilter)
                    aucGaucBuffer += AucGauc(auc = aucROC,
                        group = "adslot_type",
                        tag = x,
                        model = s"%$exp%",
                        date = date,
                        hour = hour)
                    println(s"aucROC = $aucROC ,modeltype = %$exp% , group=adslotid, tag = $x")
                }
                unionJoincvrFilter.unpersist()
            }
        }
        println("-------------------------------adslot_type-----------------------------------")

        for (x <- adclass) {
            for (exp <- exptag) {
                val unionJoincvrFilter = unionJoincvr
                  .filter(s"exptag = '$exp' and adclass = $x")
                  .coalesce(400)
                  .cache()
                if (unionJoincvrFilter.count() > 0) {
                    val aucROC = CalcMetrics.getAuc(spark, unionJoincvrFilter)
                    aucGaucBuffer += AucGauc(auc = aucROC,
                        group = "adclass",
                        tag = x.toString,
                        model = s"%$exp%",
                        date = date,
                        hour = hour)
                    println(s"aucROC = $aucROC ,modeltype = %$exp% , group=adclass, tag = $x")
                }
                unionJoincvrFilter.unpersist()
            }
        }
        println("-------------------------------adclass-----------------------------------")

        for (x <- userid) {
            for (exp <- exptag) {
                val unionJoincvrFilter = unionJoincvr
                  .filter(s"exptag = '$exp' and userid = $x")
                  .coalesce(400)
                  .cache()
                if (unionJoincvrFilter.count() > 0) {
                    val aucROC = CalcMetrics.getAuc(spark, unionJoincvrFilter)
                    aucGaucBuffer += AucGauc(auc = aucROC,
                        group = "userid",
                        tag = x.toString,
                        model = s"%$exp%",
                        date = date,
                        hour = hour)
                    println(s"aucROC = $aucROC ,modeltype = %$exp% , group=userid, tag = $x")
                }
                unionJoincvrFilter.unpersist()
            }
        }
        val aucGauc = aucGaucBuffer.toList.toDF()
        aucGauc
          .repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.novel_cvr_auc_gauc")
        unionJoincvr.unpersist()
        println("-------------------------------userid-----------------------------------")

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
                       var group:String = "",
                       var tag:String = "",
                       var model:String = "",
                       var date:String = "",
                       var hour:String = "")
}