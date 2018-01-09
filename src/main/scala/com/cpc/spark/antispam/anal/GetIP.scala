package com.cpc.spark.antispam.anal

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by wanli on 2017/8/4.
  */
object GetIP {
  def main(args: Array[String]): Unit = {

    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    var sql1 = (" SELECT searchid,ip,media_appsid,isclick,isshow from dl_cpc.cpc_union_log " +
      "where `date` ='%s' and  isfill = 1 ").format(date)

    val ipDict = Source.fromFile("/data/cpc/anal/conf/FANCY-5263", "UTF-8").getLines().filter(_.length > 0).toSeq.map{
      x =>
        (x.toLong, 1)
    }
    var ipDictRdd = ctx.sparkContext.parallelize(ipDict)

    println("count ip dict:"+ ipDictRdd.count())
    println("sql1:"+ sql1)
   var union =  ctx.sql(sql1).rdd.map{
    x =>
      val searchid = x(0).toString()
      val ip = x(1).toString()
      val media_id = x(2).toString()
      val isclick = x(3).toString().toInt
      val isshow = x(4).toString().toInt
      (searchid, ip, media_id, isclick, isshow)
  }.filter(x => x._2.length > 0).map{
    case (searchid,ip,media_id, isclick, isshow) =>
      try {
        (ipToLong(ip), (media_id, isclick, isshow))
      } catch {
       case e: Exception => null
      }
   }.filter(x => x != null)
    println("union count:"+union.count())
   /* var toResult = union.join(ipDictRdd).map{
      case (ip, ((media_id, isclick, isshow),dict)) =>
        //var isDict = dict.getOrElse(0)
        ((ip, media_id),( 1, isclick, isshow))
    }.reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3))
    .map{
      case ((ip, media_id),(req, isclick, isshow)) =>
        ip+","+media_id+ ","+req +","+isclick +","+isshow
    }*/
   var toResult = union.join(ipDictRdd).map{
     case (ip, ((media_id, isclick, isshow),dict)) =>
       //var isDict = dict.getOrElse(0)
       (ip,( 1, isclick, isshow))
   }.reduceByKey((x, y) => (x._1+y._1, x._2+y._2, x._3+y._3))
     .map{
       case (ip,(req, isclick, isshow)) =>
         ip+","+req +","+isclick +","+isshow
     }
    println("count:" + toResult.count())
    toResult.take(20).foreach(println)
    toResult.repartition(1).saveAsTextFile("/user/cpc/ipblack/" + date + ".csv")
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