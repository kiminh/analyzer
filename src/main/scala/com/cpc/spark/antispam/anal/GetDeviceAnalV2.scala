package com.cpc.spark.antispam.anal

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.antispam.anal.GetDeviceAnal.{clearReportHourData, mariadbProp, mariadbUrl}
import com.cpc.spark.antispam.log.GetMediaLog.{mariadbProp, mariadbUrl}
import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetDeviceAnalV2 {
  var mariadbUrl = ""
  val mariadbProp = new Properties()
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val date1 = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))
    var list = Seq[(Int, Int, Int)]()
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql ="SELECT id as adslot_id,media_id, user_id from adslot"
      println("sql" + sql) ;
      var rs = stmt.executeQuery(sql)
      while(rs.next()){
        list = list :+ (rs.getInt(1),rs.getInt(2),rs.getInt(3))
      }
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
    val ctx = SparkSession.builder()
      .appName("device anal same imei ")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    var uidAdslotRdd = ctx.sparkContext.parallelize(list).map{
      case (adslot_id,media_id,user_id ) =>
        (media_id, user_id )
    }.distinct()
    var sql1 = "SELECT * from  dl_cpc.cpc_union_log where `date` ='%s'  and ext['device_ids'].string_value != '' ".format(date1)
    println("sql1:"+ sql1)
    var unionRdd = ctx.sql(sql1).as[UnionLog].rdd.map(
      x => (x.media_appsid,x.adslotid,x.adslot_type, x.model, x.brand,x.os , x.ext.getOrElse("device_ids",ExtValue()).string_value,x.city)
    ).filter(x => x._7.length >0)
      .map{
        case (media_appsid, adslotid, adslot_type, model, brand, os, device_ids, city) =>
          var deviceIdsArr = device_ids.split(";")
          var imei = ""
          var androidId = ""
          var idfa = ""
          deviceIdsArr.foreach{
            x =>
              var arr = x.split(":")
              if(arr.length == 2){
                if(arr(0) == "DEVID_IMEI"){
                  imei = arr(1)
                }else if(arr(0) == "DEVID_ANDROIDID"){
                  androidId = arr(1)
                }else if(arr(0) == "DEVID_IDFA"){
                  idfa = arr(1)
                }
              }
          }
          (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city)
      }.filter{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId, idfa, city) =>
        if(os == 1 && imei.length > 0 ){
          true
        }else{
           false
        }
    }.map{
      case (media_appsid, adslotid, adslot_type, model, brand,os , imei, androidId,idfa, city) =>
        ((media_appsid.toInt, imei), 1)
    }.reduceByKey((x, y ) => x).map{
      case  ((media_appsid, imei), count) =>
        (media_appsid, imei)
    }
    var joinRdd= unionRdd.join(uidAdslotRdd)

    var imeiCount = joinRdd.map{
      case (media_appsid,(imei, user_id)) =>
        ((media_appsid,user_id), 1)
    }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid,user_id), count) =>
        (user_id, (media_appsid, count))
    }
    var imeiSameCount = joinRdd.map{
      case  (media_appsid,(imei, user_id)) =>
        ((user_id,imei), 1)
    }.reduceByKey((x,y) => x+y).filter(x => x._2 > 1).map{
      case ((user_id,imei), count) =>
        (user_id, 1)
    }.reduceByKey((x,y) => x+y)

    var toResult = imeiCount.join(imeiSameCount).map{
      case  (user_id,((media_appsid, count), samecount)) =>
        user_id + "," + media_appsid + ","  + count + "," + samecount + "," + samecount.toDouble/count.toDouble*100
        ImeiSame(
          date = date1,
          media_id = media_appsid,
          user_id = user_id,
          imei_count = count,
          same_imei_count = samecount
      )
    }
    println("count:" + toResult.count())
    clearReportHourData("report_media_imei_same", date1)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_media_imei_same", mariadbProp)
    ctx.stop()
  }
  def clearReportHourData(tbl: String, date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from union.%s where `date` = "%s"
        """.stripMargin.format(tbl, date)
      println("sql" + sql) ;
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }
  case class ImeiSame(
                       date: String = "",
                       media_id:Int = 0,
                       user_id:Int = 0,
                       imei_count:Int = 0,
                       same_imei_count:Int = 0
                     )
}