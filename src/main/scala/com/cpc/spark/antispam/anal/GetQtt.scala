package com.cpc.spark.antispam.anal

import com.cpc.spark.log.parser.UnionLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by wanli on 2017/8/4.
  */
object GetQtt {
  def main(args: Array[String]): Unit = {

    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var sql1 = (" SELECT DISTINCT member_id from gobblin.qukan_member_cheating_tag  ").format(date)
    var sql3 = (" SELECT * from dl_cpc.cpc_union_log WHERE media_appsid in (80000002,80000001) and `date`='%s' and isfill=1  ").format(date)
    val sql2 = ("SELECT device_code,member_id from gobblin.qukan_p_member_info " +
      "where day  in ('%s') group by device_code,member_id ").format(date)
    var sql4 =  """
                  |SELECT DISTINCT tr.searchid,tr.trace_type,tr.duration,
                  |un.uid,un.hour,un.adslot_type,un.ext['antispam'].int_value
                  |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_click_log as un on tr.searchid = un.searchid
                  |WHERE  tr.`date` = '%s'  and un.`date` = '%s' and un.media_appsid in (80000002,80000001)
                """.stripMargin.format(date,date)

    println("sql1:"+ sql1)
    println("sql2:"+ sql2)
    var qttRdd =  ctx.sql(sql1).rdd.map{
    x =>
      val member_id = x(0).toString()
      (member_id,1)
    }
    var qttRdd2 =  ctx.sql(sql2).rdd.map{
      x =>
        val device_code = x(0).toString()
        val member_id = x(1).toString()
        (member_id,device_code)
    }
    var qttRddResult = qttRdd.join(qttRdd2).map{
      case (member_id,(x,device_code)) =>
        (device_code, member_id)
    }
    var union =  ctx.sql(sql3).as[UnionLog].rdd.map{
      x =>
        (x.searchid,(x.media_appsid,x.adslotid,x.adslot_type,x.hour,x.uid, x.isshow,x.isclick))
    }
    println("sql4:"+sql4)
    val traceRdd = ctx.sql(sql4).rdd
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
          var searchId = x.getString(0)
  /*        var device = x.getString(3)
          var hour = x.getString(4)
          var adslotType = x.getInt(5)
          var antispam = x.getInt(6)*/
          (searchId,(load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10)
      }
      .cache()

    var unionTrace =union.leftOuterJoin(traceRdd).map{
     // def map[U](f: ((String, ((String, String, Int, String, String, Int, Int), Option[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)])))
      case (searchid,((media_appsid,adslotid,adslot_type,hour,uid, isshow,isclick), trace:Option[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]))=>
        var trace2 = trace.getOrElse((0,0,0,0,0,0,0,0,0,0))
        ((media_appsid,adslotid,adslot_type,hour,uid),
          (isshow,isclick,trace2._1, trace2._2, trace2._3, trace2._4, trace2._5, trace2._6, trace2._7, trace2._8, trace2._9, trace2._10))
    }.reduceByKey {
      (a, b) =>
        (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9,
          a._10 + b._10, a._11 + b._11, a._12 + b._12)
    }

   val filterUnion =  unionTrace.map{
      case ((media_appsid,adslotid,adslot_type,hour,uid),
      (isshow,isclick,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)) =>
        (uid,
          (media_appsid,adslotid,adslot_type,hour, isshow,isclick,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
    }.join(qttRddResult).map{
      case  (uid,((media_appsid,adslotid,adslot_type,hour, isshow,isclick,load,
      active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120),x))=>
        ((media_appsid,adslotid,adslot_type,hour), (isshow,isclick,load,
          active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
    }.reduceByKey {
      (a, b) =>
        (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9,
          a._10 + b._10, a._11 + b._11, a._12 + b._12)
    }
    filterUnion.take(10).foreach(println)
   val allUnion =  unionTrace.map{
      case ((media_appsid,adslotid,adslot_type,hour,uid),
      (isshow,isclick,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)) =>
        ((media_appsid,adslotid,adslot_type,hour),
          (isshow,isclick,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120))
    }.reduceByKey {
      (a, b) =>
        (a._1 + b._1,a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9,
          a._10 + b._10, a._11 + b._11, a._12 + b._12)
    }
    allUnion.join(filterUnion).map{
      case  ((media_appsid,adslotid,adslot_type,hour),
      ((isshow,isclick,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120),
      (isshow2,isclick2,load2, active2, buttonClick2, press2, stay12, stay52, stay102, stay302, stay602, stay1202))) =>
        hour +","+ media_appsid+","+adslotid+","+adslot_type+","+isshow+","+isclick+","+load+","+active+","+
          buttonClick+","+press+","+isshow2+","+isclick2+","+load2+","+active2+","+buttonClick2+","+press2
    }.collect().foreach(println)

    ctx.stop()
}
  def searchIp(dict:Seq[IpDict], ipint:Long): Long ={
    var min = 0
    var max:Int = dict.length - 1
    var mid :Int =0
    // 导入以下包
    import scala.util.control._

    // 创建 Breaks 对象
    val loop = new Breaks

    // 在 breakable 中循环
    loop.breakable{
      // 循环
      while (min <= max ){
        mid = (min + max) / 2
        var  ipsec = dict(mid)
        if (ipsec.Start > ipint) {
          max = mid - 1
        }else if (ipsec.End < ipint) {
          min = mid + 1
        }else{
          loop.break
        }
      }
    }

    if (min > max) {
      return 0
    }
    var ipsec = dict(mid)
    if (ipsec.Start <= ipint && ipsec.End >= ipint ){
      return ipsec.Isp
    } else {
      return 0
    }
  }
  def ipToLong(ipAddress: String): Long = {
    val ipAddressInArray = ipAddress.split("\\.")
    var result:Long = 0
    var i = 0
    while (i < ipAddressInArray.length) {
       val power = 3 - i
       var ip = ipAddressInArray(i).toLong
        result =  result + ip * Math.pow(256, power).toLong
        i += 1
    }

    result
  }
  case class IpDict(
                 Start: Long = 0,
                 End: Long = 0,
                  Isp: Long = 0
                        )

}