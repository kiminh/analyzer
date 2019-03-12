package com.cpc.spark.small.tool

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetUidReport {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)
    val dateDay = args(0)
    val ctx = SparkSession.builder()
      .appName("device analy ")
      .enableHiveSupport()
      .getOrCreate()
    val sql1 = " SELECT field[\"device\"].string_type ,day from dl_lechuan.qukan_daily_new_user_p
    val sql2 =
      s"""
         |SELECT
         |  uid
         |  , sum(isclick)
         |  , sum(isshow)
         |from dl_cpc.cpc_basedata_union_events
         |where day='$dateDay'
         |  and isfill>0
         |  and media_appsid in ("80000001","80000002")
         |GROUP BY uid
       """.stripMargin

    println("sql:"+sql1)
    println("sq2:"+sql2)

    val rdd1 = ctx.sql(sql1).rdd.map{
      x =>
        val uid : String =  x(0).toString()
        val day : String = x(1).toString()
        (uid,day)
    }.reduceByKey((x,y) => x)
    val rdd2 = ctx.sql(sql2).rdd
      .map {
        x =>
          val uid : String =  x(0).toString()
          val click : Int = x(1).toString().toInt
          val show : Int = x(2).toString().toInt
          (uid,( click, show))
        }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2))
      rdd2.map{
        case (uid,( click, show)) =>
          ("old",( click, show))
      }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))
    val all = rdd2.join(rdd1)
    val dev05 = rdd2.join(rdd1.filter(x => x._2 < "2017-06-01"))
    val dev06 = rdd2.join(rdd1.filter(x => x._2 < "2017-07-01" && x._2 >= "2017-06-01"))
    val dev07 = rdd2.join(rdd1.filter(x => x._2 < "2017-08-01" && x._2 >= "2017-07-01"))
    val dev08 = rdd2.join(rdd1.filter(x => x._2 < "2017-09-01" && x._2 >= "2017-08-01"))
    val dev09 = rdd2.join(rdd1.filter(x => x._2 < "2017-09-10" && x._2 >= "2017-09-01"))
    println("all:"+ all.count())
    println("~06-01:"+ dev05.count())
    println("06-01~07-01:"+ dev06.count())
    println("07-01~08-01:"+ dev07.count())
    println("08-01~09-01:"+ dev08.count())
    println("09-01~09-10:"+ dev09.count())
    rdd2.join(rdd1.filter(x => x._2 > "2017-06-01")).map{
      case (uid,(( click, show), day)) =>
        ("06-01-new",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))

    dev05.map{
      case (uid,(( click, show), day)) =>
        ("~06-01:",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))
    dev06.map{
      case (uid,(( click, show), day)) =>
        ("06-01~07-01:",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))

    dev07.map{
      case (uid,(( click, show), day)) =>
        ("07-01~08-01:",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))

    dev08.map{
      case (uid,(( click, show), day)) =>
        ("08-01~09-01:",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))

    dev09.map{
      case (uid,(( click, show), day)) =>
        ("09-01~09-10:",( click, show))
    }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))

    /*  println("old:"+ rdd2.count())
      println("06-01-new:"+ rdd2.join(rdd1.filter(x => x._2 > "2017-06-01")).count())
      println("07-01-new:"+ rdd2.join(rdd1.filter(x => x._2 > "2017-07-01")).count())
      println("08-01-new:"+ rdd2.join(rdd1.filter(x => x._2 > "2017-08-01")).count())
      println("09-01-new:"+ rdd2.join(rdd1.filter(x => x._2 > "2017-09-01")).count())
     rdd2.join(rdd1.filter(x => x._2 > "2017-06-01")).map{
         case (uid,(( click, show), day)) =>
           ("06-01-new",( click, show))
       }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))
      rdd2.join(rdd1.filter(x => x._2 > "2017-07-01")).map{
        case (uid,(( click, show), day)) =>
          ("07-01-new",( click, show))
      }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))
      rdd2.join(rdd1.filter(x => x._2 > "2017-08-01")).map{
        case (uid,(( click, show), day)) =>
          ("08-01-new",( click, show))
      }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))
      rdd2.join(rdd1.filter(x => x._2 > "2017-09-01")).map{
        case (uid,(( click, show), day)) =>
          ("09-01-new",( click, show))
      }.reduceByKey((x,y) => (x._1+ y._1, x._2+ y._2)).collect().foreach(x =>println(x + "ctr"+ x._2._1.toDouble/ x._2._2.toDouble*100))*/
  }
}