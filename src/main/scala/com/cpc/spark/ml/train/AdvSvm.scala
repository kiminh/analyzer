package com.cpc.spark.ml.train

import com.cpc.spark.log.parser.UnionLog
import org.apache.spark.sql.SparkSession
import scala.util.hashing.MurmurHash3.stringHash

object AdvSvm {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: GenerateAdvSvm <hive_table> <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    val table = args(0)
    val date = args(1)
    val hour = args(2)
    val sparkSession = SparkSession.builder()
      .appName("GenerateAdvSvm from %s %s".format(table, date))
      .enableHiveSupport()
      .getOrCreate()
    import sparkSession.implicits._
    val unionLog = sparkSession.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and hour = "%s" and isfill = 1 and adslotid > 0
       """.stripMargin.format(table, date, hour))
      .as[UnionLog]
    val svmLine = unionLog.map {
      x =>
        /*
        val isclick = x.isclick
        var svmString: String = isclick.toString
        svmString += " 1:" + x.network
        svmString += " 2:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.ip))
        svmString += " 3:" + x.media_type
        svmString += " 4:" + x.media_appsid
        svmString += " 5:" + x.bid
        svmString += " 6:" + x.ideaid
        svmString += " 7:" + x.unitid
        svmString += " 8:" + x.planid
        svmString += " 9:" + x.userid
        svmString += " 10:" + x.country
        svmString += " 11:" + x.province
        svmString += " 12:" + x.city
        svmString += " 13:" + x.isp
        svmString += " 14:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.uid))
        svmString += " 15:" + x.coin
        svmString += " 16:" + Math.abs(scala.util.hashing.MurmurHash3.stringHash(x.date))
        svmString += " 17:" + x.hour
        svmString += " 18:" + x.adslotid
        svmString += " 19:" + x.adslot_type
        svmString += " 20:" + x.adtype
        svmString += " 21:" + x.interaction
        svmString
        */

        //network
        //isp

        //planid unitid ideaid

        //interaction

        //country + city

        //hour

        //medaid
        //adslot_id


        var svm = x.isclick.toString
        svm += " 1:" + x.network
        svm += " 2:" + x.isp
        svm += " 3:" + stringHash("%d-%d-%d".format(x.planid, x.unitid, x.ideaid))
        svm += " 4:" + x.interaction
        svm += " 5:" + stringHash("%d-%d".format(x.country, x.city))
        svm += " 6:" + x.hour
        svm += " 7:" + x.media_appsid
        svm += " 8:" + x.adslotid
        svm
    }

    svmLine.rdd.saveAsTextFile("/user/cpc/svmdata/" + date + "-" + hour)
  }
}

