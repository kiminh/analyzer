package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetAntispam2 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val date = args(0)
    val dateDay = args(1)

    val ctx = SparkSession.builder()
      .appName("cvr report")
      .enableHiveSupport()
      .getOrCreate()
    val hivesql = "select device from  rpt_ac.qukan_item_clk  where thedate > \"%s\" and rate > 0.7 and rate < 999 group by device ".format(date)

    val sql =
      s"""
         |SELECT
         |  uid
         |  , isclick
         |  , isshow
         |  , adslot_type
         |  , hour
         |from dl_cpc.cpc_basedata_union_events
         |where day="$dateDay"
         |  and adslot_type=1
       """.stripMargin
    
    println("sql:"+sql)
    println("hivesql:"+hivesql)
    val union = ctx.sql(sql).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          val click : Int = x(1).toString().toInt
          val show : Int = x(2).toString().toInt
          val adslotType : Int = x(3).toString().toInt
          val hour  = x(4).toString()
          (uid,(click, show,adslotType,hour))
        }.cache()
    val antispamRdd = ctx.sql(hivesql).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          (uid,1)
      }.cache()
    union.join(antispamRdd).map{
      case (uid,((click, show,adslotType,hour),x)) =>
        (hour,(show, click))
    }.reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).map{
      case (hour,(show, click)) =>
        (hour,(show, click,click.toDouble/show.toDouble * 100))
    }.collect().foreach(println)

  }
}