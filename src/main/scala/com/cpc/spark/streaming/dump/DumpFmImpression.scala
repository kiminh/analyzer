package com.cpc.spark.streaming.dump

import java.text.SimpleDateFormat

import data.mlevent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * author: huazhenhao
  * date: 11/7/18
  */

case class FmImpLog(
                     var timestamp: Long = 0,
                     var insertionID: String = "", // unique key for each ad insertion
                     var requestID: String = "",
                     var userID: String = "", // client user ID (not advertiser ID)
                     var actionMap: collection.Map[Int, Int] = null,
                     var thedate: String = "",
                     var thehour: String = "",
                     var theminute: String = ""
                   )


object DumpFmImpression {

  def main(args: Array[String]): Unit = {
    val runner = new KafkaDumpBase[FmImpLog](
      "fm_impression",
      "FM_IMP_LOG_KAFKA_OFFSET",
      "parse_cpc_fm_imp_minute") {
      override def parseFunc(bytes: Array[Byte]): FmImpLog = {
        val aae = mlevent.AdActionEvent.parseFrom(bytes)
        val timestamp = aae.timestamp
        val date = new SimpleDateFormat("yyyy-MM-dd").format(timestamp)
        val hour = new SimpleDateFormat("HH").format(timestamp)
        val minute = new SimpleDateFormat("mm").format(timestamp).charAt(0) + "0"

        val insertionID = aae.insertionID
        val requestID = aae.requestID
        val userID = aae.userID
        val actionMap: collection.Map[Int, Int] = aae.actionMap

        FmImpLog(timestamp, insertionID, requestID, userID, actionMap, date, hour, minute)
      }

      override def getTimeKeyFunc(element: FmImpLog): (String, String, String) = {
        (element.thedate, element.thehour, element.theminute)
      }

      override def getDF(rdd: RDD[FmImpLog], spark: SparkSession): DataFrame = {
        spark.createDataFrame(rdd)
      }
    }
    runner.run()
  }
}

