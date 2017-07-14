package com.cpc.spark.log.report

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Roy on 2017/4/26.
  */
object GetPhoneReportV2 {

  var mariadbUrl = ""
  val mariadbProp = new Properties()
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetPhoneReport <Date>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val conf = ConfigFactory.load()
    mariadbUrl = "jdbc:mysql://10.9.164.80:3306/dmp"
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val ctx = SparkSession.builder()
      .appName("cpc get phone brand and model from  date:%s".format(date1))
      .enableHiveSupport()
      .getOrCreate()
    val rdd = ctx.sql( s"""
         |SELECT brand,model,count(*)  as value FROM dl_cpc.cpc_union_log where `date`="%s"
         | and  (ext['phone_price'].int_value = 0 or  ext['phone_price'] is null) GROUP BY brand,model """.stripMargin.format(date1)).rdd
    val phoneRdd = rdd.filter(x =>  x(1).toString.length > 0  && x(1).toString.length < 100 && x(1).toString.indexOf("\\x") >= 0 ).map{
      p =>
        var model = p(1).toString
        val model_lower = model.replace("-","_").toLowerCase()
        val brand = p(0).toString
        (model_lower, Phone2(brand, model, model_lower, date1, p(2).toString.toInt))
    }.reduceByKey((x, y) => y).map(x => x._2).distinct()

    println("*********demo**********")
    phoneRdd.collect().foreach(println)

    ctx.createDataFrame(phoneRdd)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "dmp.phoneprice", mariadbProp)
    ctx.stop()
  }
}
case class Phone2(brand: String = "", model: String = "", model_lower: String = "", date: String = "", value: BigInt = 0)