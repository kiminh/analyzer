package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by wanli on 2017/8/4.
  */
object GetCvrInfo3 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)

    val dateDay = args(0)

    val ctx = SparkSession.builder()
      .appName("cvr report")
      .enableHiveSupport()
      .getOrCreate()
   val sql =  """
             |SELECT DISTINCT tr.searchid,tr.trace_type,tr.duration,tr.hour,tr.date
             |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
             |WHERE  tr.`date` in(%s)  and un.`date` in(%s) and un.adslot_type =1 and   un.ext['antispam'].int_value =1
           """.stripMargin.format(dateDay,dateDay)
    println("sql:"+sql)
    val traceRdd = ctx.sql(sql).rdd
      .map {
        x =>
          var trace_type = ""
          var load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120 = 0

          x.getString(1) match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            case "buttonClick" => buttonClick += 1
            case "press" => press += 1
            case "stay" => x.getInt(2) match {
              case 1 => stay1 += 1
              case 5 => stay5 += 1
              case 10 => stay10 += 1
              case 30 => stay30 += 1
              case 60 => stay60 += 1
              case 120 => stay120 += 1
              case _ =>
            }
            case _ =>
          }
          (x.getString(0) ,(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120,x.getString(3),x.getString(4)))
        }
        .reduceByKey {
          (a, b) =>
            (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10,a._11,a._12)
        }
        .cache()

      val xActionRdd = traceRdd.map {
        x =>
          var xAction = 0
          if ((x._2._2 + x._2._3) > 0) {
            xAction = 1
          }
          (x._1, xAction)
      }
    val joinRdd = traceRdd.join(xActionRdd).map{
      case (searchId, ((load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120,hour,date), xaction)) =>
        ((date,hour),(load, xaction))
    }.reduceByKey((x, y) => (x._1+y._1, x._2+y._2)).map{
      case  ((date,hour),(load, xaction)) =>
        ((date,hour),(load,xaction,xaction.toDouble/load.toDouble * 100))
        var  loadXaction = (xaction.toDouble/load.toDouble * 100).formatted("%.3f")
          "%s,%s,%d,%d,%s".format(date, hour, load, xaction, loadXaction)
    }.collect().foreach(println)
  }
}