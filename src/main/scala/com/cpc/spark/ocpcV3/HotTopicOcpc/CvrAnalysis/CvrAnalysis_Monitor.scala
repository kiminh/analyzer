package com.cpc.spark.ocpcV3.HotTopicOcpc.CvrAnalysis

import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics

object CvrAnalysis_Monitor {

  def main(args: Array[String]): Unit = {

    val date = args(0)
    val spark = SparkSession.builder().appName(s"HotTopicCvrAuc date = $date").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val media = "hot_topic_cvr"
    val sql =
      s"""
         |select
         |  ext['exp_cvr'].int_value                 as score,
         |  iscvr                                    as label,
         |  ext_string['cvr_model_name']             as cvr_model_name,
         |  cast(ext['adclass'].int_value as string) as adclass,
         |  cast(adslotid as string)                 as adslot_id,
         |  cast(userid as string)                   as userid,
         |  cast(adslot_type as string)              as adslot_type
         |from (
         |     select
         |       searchid,
         |       ext,
         |       ext_string,
         |       adslotid,
         |       userid,
         |       adslot_type
         |     from dl_cpc.cpc_hot_topic_union_log
         |    where `date` = '$date'
         |      and media_appsid in ('80002819')
         |      and adsrc  = 1
         |      and isshow = 1
         |      and ideaid > 0
         |      and userid > 0
         |      and (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
         | ) ta
         |join (
         | select
         |   searchid,
         |   label2 as iscvr --是否转化
         |
         |  from dl_cpc.ml_cvr_feature_v1
         | WHERE `date` = '$date'
         | ) tb
         |on ta.searchid = tb.searchid
             """.stripMargin

    val union = spark.sql(sql).cache() //union: Dataset
    val DetailAucListBuffer = scala.collection.mutable.ListBuffer[DetailAuc]()
    val time1=System.currentTimeMillis()
    //分模型
    val cvrModelNames = union.select("cvr_model_name").distinct().collect().map(x => x.getAs[String]("cvr_model_name"))  //运行到"collect()"，得到一个row结构（有列名），后面括号中的"cvr_model_name"表示将该列取出来并转化为String类型。
    println("cvrModelNames 's num is " + cvrModelNames.length)
    for (cvrModelName <- cvrModelNames) {
      val cvrModelUnion = union.filter(s"cvr_model_name = '$cvrModelName'")
      val cvrModelAuc = CalcMetrics.getAuc(spark,cvrModelUnion)
      DetailAucListBuffer += DetailAuc(
        name = cvrModelName,
        auc = cvrModelAuc,
        sum = cvrModelUnion.count().toDouble,
        day = date,
        media = media,
        tag = "cvr_model_name")
    }
    val time2=System.currentTimeMillis()
    println("cvrModelNames cost is " + (time2-time1))
    //    分行业
    //            val adclassList = union.select("adclass")
    //              .distinct()
    //              .collect()
    //              .map(x => x.getAs[String]("adclass"))
    //            println("adclassList 's num is " + adclassList.length)
    //            for (adclass <- adclassList) {
    //                val adclassUnion = union.filter(s"adclass = '$adclass'")
    //                val adclassAuc = CalcMetrics.getAuc(spark,adclassUnion)
    //
    //                DetailAucListBuffer += DetailAuc(tag = "cvr_model_name",
    //                    name = adclass,
    //                    auc = adclassAuc,
    //                    sum = adclassUnion.count().toDouble,
    //                    media = media,
    //                    day = date)
    //            }
    val adclassAucList = CalcMetrics.getGauc(spark,union,"adclass").rdd
      .map(x => {
        val adclass = x.getAs[String]("name")
        val auc = x.getAs[Double]("auc")
        val sum = x.getAs[Double]("sum")
        (adclass,auc,sum)
      })
      .collect()
    for(adclassAuc <- adclassAucList) {
      DetailAucListBuffer += DetailAuc(tag = "adclass",
        name = adclassAuc._1,
        auc = adclassAuc._2,
        sum = adclassAuc._3,
        media = media,
        day = date)
    }
    val time3=System.currentTimeMillis()
    println("adclassAucList cost is " + (time3-time2))
    //分栏位
    val adslotIds = union.select("adslot_id")
      .distinct()
      .collect()
      .map(x => x.getAs[String]("adslot_id"))
    println("adslotIds 's num is " + adslotIds.length)
    for (adslotId <- adslotIds) {
      val adslotIdUnion = union.filter(s"adslot_id = '$adslotId'")
      val adslotIdAuc = CalcMetrics.getAuc(spark,adslotIdUnion)
      DetailAucListBuffer += DetailAuc(
        name = adslotId,
        auc = adslotIdAuc,
        sum = adslotIdUnion.count().toDouble,
        day = date,
        media = media,
        tag = "adslot_id")
    }
    val time4=System.currentTimeMillis()
    println("adslotIds cost is " + (time4-time3))
    //分广告主
    val userIdAucList = CalcMetrics.getGauc(spark,union,"userid").rdd
      .map(x => {
        val userid = x.getAs[String]("name")
        val auc = x.getAs[Double]("auc")
        val sum = x.getAs[Double]("sum")
        (userid,auc,sum)
      })
      .collect()
    for(userIdAuc <- userIdAucList) {
      DetailAucListBuffer += DetailAuc(
        name = userIdAuc._1,
        auc = userIdAuc._2,
        sum = userIdAuc._3,
        day = date,
        media = media,
        tag = "userid")
    }
    val time5=System.currentTimeMillis()
    println("userIdAucList cost is " + (time5-time4))
    //分栏位类型
    val adslotTypes = union.select("adslot_type")
      .distinct()
      .collect()
      .map(x => x.getAs[String]("adslot_type"))
    println("adslotTypes 's num is " + adslotTypes.length)
    for (adslotType <- adslotTypes) {
      val adslotIdUnion = union.filter(s"adslot_type = '$adslotType'")
      val adslotIdAuc = CalcMetrics.getAuc(spark,adslotIdUnion)
      DetailAucListBuffer += DetailAuc(
        name = adslotType,
        auc = adslotIdAuc,
        sum = adslotIdUnion.count().toDouble,
        day = date,
        media = media,
        tag = "adslot_type")
    }
    val time6=System.currentTimeMillis()
    println("adslotTypes cost is " + (time6-time5))

    val detailAuc = DetailAucListBuffer.toList.toDF()

    detailAuc.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.cpc_detail_auc")

  }
  case class DetailAuc(var name:String = "",
                       var auc:Double = 0,
                       var sum:Double = 0,
                       var day:String = "",
                       var media:String = "",
                       var tag:String = "")

}
