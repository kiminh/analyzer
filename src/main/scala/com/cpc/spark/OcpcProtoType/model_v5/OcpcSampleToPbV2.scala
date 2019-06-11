package com.cpc.spark.OcpcProtoType.model_v5

import java.io.FileOutputStream

import org.apache.spark.sql.DataFrame
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer


object OcpcSampleToPbV2 {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string identifier = 1;
    int32 conversiongoal = 2;
    double kvalue = 3;
    double cpagiven = 4;
    int64 cvrcnt = 5;
    对于明投广告，cpagiven=1， cvrcnt使用ocpc广告记录进行关联，k需要进行计算

    将文件从dl_cpc.ocpc_pcoc_jfb_hourly表中抽出，存入pb文件，需要过滤条件：
    kvalue>0
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12 qtt_demo 1
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isKnown = args(3).toInt

    val fileName = "ocpc_params_qtt.pb"

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_post_cvr_unitid_hourly
         |WHERE
         |  `date` = '2019-06-03'
         |AND
         |  `hour` = '12'
         |AND
         |  version = 'qtt_demo'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)


//    savePbPack(data, fileName, spark)
  }



  def savePbPack(data: DataFrame, fileName: String, spark: SparkSession): Unit = {
    /*
    oCPCQTT&unitid&isHiddenOcpc
    string   key = 1;
    int32    conversionGoal = 2;
    double   cvrCalFactor = 3;
    double   jfbFactor = 4;
    double   smoothFactor = 5;
    double   postCvr = 6;
    double   cpaGiven = 7;
    double   cpaSuggest = 8;
    double   paramT = 9;
    double   highBidFactor = 10;
    double   lowBidFactor = 11;
    int64    ocpcMincpm = 12;
    int64    ocpcMinbid = 13;
    int64    cpcbid = 14;
    int64    maxbid = 15;
     */
//    val key = "oCPCQtt&270&0"
//    val conversionGoal = 3
//    val cvrCalFactor = 0.5
//    val jfbFactor = 1.1
//    val smoothFactor = 0.5
//    val postCvr = 0.032
//    val cpaGiven = 1000.0
//    val cpaSuggest = 1200.0
//    val paramT = 2.0
//    val highBidFactor = 1.1
//    val lowBidFactor = 1.0
//    val minCPM = 0
//    val minBid = 0
//    val cpcbid = 12
//    val maxbid = 0
    var list = new ListBuffer[SingleItem]
    var cnt = 0

    for (record <- data.collect()) {
      val identifier = record.getAs[String]("identifier")
      val key = "oCPCQtt&" + identifier + "&0"
      val conversionGoal = 3
      val cvrCalFactor = record.getAs[Double]("cali_value")
      val jfbFactor = 1.1
      val smoothFactor = 0.5
      val postCvr = record.getAs[Double]("cvr3")
      val cpaGiven = 1000.0
      val cpaSuggest = record.getAs[Double]("cpa_suggest")
      val paramT = 2.0
      val highBidFactor = 1.1
      val lowBidFactor = 1.0
      val minCPM = 0
      val minBid = 0
      val cpcbid = 12
      val maxbid = 0

      if (cnt % 100 == 0) {
        println(s"key:$key, conversionGoal:$conversionGoal, cvrCalFactor:$cvrCalFactor, jfbFactor:$jfbFactor, smoothFactor:$smoothFactor, postCvr:$postCvr, cpaGiven:$cpaGiven, cpaSuggest:$cpaSuggest, paramT:$paramT, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, minCPM:$minCPM, minBid:$minBid, cpcbid:$cpcbid, maxbid:$maxbid")
      }
      cnt += 1

      //      string identifier = 1;
      //      int32 conversiongoal = 2;
      //      double kvalue = 3;
      //      double cpagiven = 4;
      //      int64 cvrcnt = 5;

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
        ocpcMincpm = minCPM,
        ocpcMinbid = minBid,
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
    adRecordList.writeTo(new FileOutputStream(fileName))

    println("complete save data into protobuffer")

  }

}

