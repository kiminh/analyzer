package com.cpc.spark.log.report

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.TraceReportLog
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Roy on 2017/4/26.
  */
object GetPhoneReport {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: GetPhoneReport <startDate> <endDate> <compareDate>
           |
        """.stripMargin)
      System.exit(1)
    }
    val startDate = args(0)
    val endDate = args(1)
    val compareDate = args(2)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get phone brand and model from  date:%s to  date:%s  and compare date: %s".format(startDate, endDate, compareDate))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    val rangRdd = ctx.sql(
      s"""
           SELECT brand,model,count(*) as num from dl_cpc.cpc_union_log where `date` >= "%s" and `date` <= "%s"  GROUP BY brand,model
         """.stripMargin.format(startDate, endDate)).as[Phone].rdd
    val oneDayRdd = ctx.sql(
      s"""
           SELECT brand,model,count(*) as num from dl_cpc.cpc_union_log where `date` = "%s"  GROUP BY brand,model
         """.stripMargin.format(compareDate)).as[Phone].rdd

    println("*********rangData**********")
    val rangeData = rangRdd.collect()
    rangeData.foreach(print)
    println("*********oneDayData**********")
    val oneDayData = oneDayRdd.collect()
    oneDayData.foreach(print)
    val intersection = ArrayBuffer[Phone]()
    for (phone1 <- rangeData){
      for (phone2 <- oneDayData){
        if(phone1.brand.equals(phone2.brand)){
          if(phone1.model.equals(phone2.model)){
            intersection += phone1
          }
        }
      }
    }
    println("*********stardate to enddate 范围内的数据在compareDate数据的比例**********")
    val rate = intersection.length.toDouble/oneDayData.length.toDouble
    println("rangeCount:" + rangeData.length)
    println("compareCount:" + oneDayData.length)
    println("intersectionCount:" + intersection.length)
    println("rate:" + rate)
    rangRdd.map(x => x.brand + "," + x.model + "," + x.num).repartition(1)
      .toDF()
      .write
      .mode(SaveMode.Overwrite)
      .text("/user/cpc/phone/" + startDate + "-" + endDate + "/")
    ctx.close()
  }
}
case class Phone(brand: String = "", model: String = "", num: BigInt = 0)