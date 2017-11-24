package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetAntispam4 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val predict = args(1).toFloat
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("model user anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    var sql1 = (" SELECT antispam_rules,media_appsid,count(distinct searchid) as num  from dl_cpc.cpc_click_log " +
      "where `date` ='%s' and isclick=1 and antispam_rules != \"DUP_SEARCH_ID\"  group by media_appsid,antispam_rules ").format(date)

    println("sql1:"+ sql1)
    var union =  ctx.sql(sql1).rdd.map{
      x =>
        val antispam_rules = x(0).toString()
        val media_appsid = x(1).toString()
        val num= x(2).toString().toInt
        (media_appsid,antispam_rules,num)
    }
    var  all = union.map(x => (x._1,x._3)).reduceByKey((x,y) => (x +y))
    union.map(x => (x._1,(x._2, x._3))).join(all).map{
      case (media_appsid,((antispam_rules,num), total)) =>
        media_appsid+","+antispam_rules.replace(",","-")+","+num +","+total+","+num.toFloat/total.toFloat*100
    }.collect().foreach(println)
  }
}