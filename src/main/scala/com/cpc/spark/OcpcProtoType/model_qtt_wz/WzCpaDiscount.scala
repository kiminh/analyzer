package com.cpc.spark.OcpcProtoType.model_qtt_wz

import java.io.FileOutputStream
import java.util.Properties

import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object WzCpaDiscount {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    key值这样设计：
    qtt_wz&adslot_type&adtype
    novel_wz&adslot_type&adtype
    66_wz&adslot_type&adtype
    qjp_wz&adslot_type&adtype

    adtype 文本 1 1
    adtype 大图 2 2
    adtype 图文 3 5
    adtype 组图 4 8
    adtype 互动 5 9
    adtype 开屏 6 10
    adtype 横幅 7 11
    adtype 横版视频 8 4
    adtype 激励 9 12
    adtype 竖版视频 10 13
    adslottype 列表页 1 1
    adslottype 详情页 2 2
    adslottype 互动页 3 3
    adslottype 开屏 4 4
    adslottype 横幅 5 14
    adslottype 视频 6 12
    adslottype 激励 7 9
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val data = ((1,))

    println("NewK")
    println(cvrData.count())
    cvrData.show(10)

    // 组装数据

    resultDF.show(10)

    resultDF
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_novel_pb_hourly")

    val resultUnion = spark.table("dl_cpc.ocpc_novel_pb_once").union(resultDF)
    savePbPack(resultUnion)
  }

  def savePbPack(dataset: DataFrame): Unit = {
    var list = new ListBuffer[SingleItem]
    val filename = "ocpc_params_novel.pb"
    println("size of the dataframe")
    println(dataset.count)
    println(s"filename: $filename")
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val identifier = record.getAs[Int]("identifier").toString
      val version = record.getAs[String]("version")
      var HiddenOcpc = 0
      if(version == "novel_v3") HiddenOcpc = 1
      val key = "oCPCNovel&" + identifier + "&" + HiddenOcpc
      val conversionGoal = record.getAs[Int]("conversion_goal")
      val cvrCalFactor = record.getAs[Double]("cvrcalfactor")
      val jfbFactor = record.getAs[Double]("kvalue")
      val smoothFactor = record.getAs[Double]("smoothfactor")
      val postCvr = record.getAs[Double]("post_cvr")
      val cpaGiven = record.getAs[Double]("cpagiven")
      val cpaSuggest = 0.0
      val paramT = 0.0
      val highBidFactor = 0.0
      val lowBidFactor = 0.0
      val ocpcMincpm = 0
      val ocpcMinbid = 0
      val cpcbid = 0
      val maxbid = record.getAs[Double]("maxbid").toInt

      if (cnt % 100 == 0) {
        println(s"key: $key,conversionGoal: $conversionGoal, cvrCalFactor:$cvrCalFactor,jfbFactor:$jfbFactor, postCvr:$postCvr, smoothFactor:$smoothFactor," +
          s"cpaGiven: $cpaGiven,cpaSuggest: $cpaSuggest, paramT: $paramT, highBidFactor: $highBidFactor, lowBidFactor:$lowBidFactor," +
          s"ocpcMincpm: $ocpcMincpm, ocpcMinbid:$ocpcMinbid, cpcbid:$cpcbid,maxbid :$maxbid ")
      }
      cnt += 1

      val currentItem = SingleItem(
        key = key,
        conversionGoal = conversionGoal,
        cvrCalFactor = cvrCalFactor,
        jfbFactor = jfbFactor,
        smoothFactor = smoothFactor,
        postCvr = postCvr,
        cpaGiven = cpaGiven,
        cpaSuggest = cpaSuggest,
        paramT = paramT,
        highBidFactor = highBidFactor,
        lowBidFactor = lowBidFactor,
        ocpcMincpm = ocpcMincpm,
        ocpcMinbid = ocpcMinbid,
        cpcbid = cpcbid,
        maxbid = maxbid
      )
      list += currentItem

    }
    val result = list.toArray[SingleItem]
    val adRecordList = OcpcParamsList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }
}


