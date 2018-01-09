package com.cpc.spark.antispam.anal

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.antispam.anal.GetDeviceAnal.{clearReportHourData, mariadbProp, mariadbUrl}
import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetDeviceAnalV3 {
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

    val ctx = SparkSession.builder()
      .appName("device anal ip imei")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    var sql1 = "SELECT * from  dl_cpc.cpc_union_log where `date` ='%s'  and ext['device_ids'].string_value != '' ".format(date1)
    println("sql1:"+ sql1)
    var unionRdd = ctx.sql(sql1).as[UnionLog].rdd.map(
      x => (x.media_appsid,x.adslotid,x.adslot_type, x.ip , x.ext.getOrElse("device_ids",ExtValue()).string_value)
    ).filter(x => x._5.length >0)
      .map{
        case (media_appsid, adslotid, adslot_type, ip, device_ids) =>
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
          (media_appsid, adslotid, adslot_type, ip , imei)
      }.filter{
      case (media_appsid, adslotid, adslot_type, ip , imei) =>
        if( imei.length > 0 ){
          true
        }else{
           false
        }
    }
   val ipRdd =  unionRdd.map{
      case (media_appsid, adslotid, adslot_type, ip , imei) =>
        ((media_appsid, adslotid, adslot_type,ip), 1)
    }.reduceByKey((x, y) => x).map{
      case ((media_appsid, adslotid, adslot_type, ip ), count) =>
        ((media_appsid, adslotid, adslot_type), 1 )
    }.reduceByKey((x, y) => x + y)

    val imeiRdd =  unionRdd.map{
      case (media_appsid, adslotid, adslot_type, ip , imei) =>
        ((media_appsid, adslotid, adslot_type, imei), 1)
    }.reduceByKey((x, y) => x).map{
      case ((media_appsid, adslotid, adslot_type, imei), count) =>
        ((media_appsid, adslotid, adslot_type), 1 )
    }.reduceByKey((x, y) => x + y)


    var toResult = ipRdd.join(imeiRdd).map{
      case ((media_appsid, adslotid, adslot_type),(ip, imei)) =>
        media_appsid + "," + adslotid + "," + adslot_type + "," + ip + "," + imei + ","+ ip.toDouble/imei.toDouble*100
        ImeiDiff(
          date = date1,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type,
          imei_count = imei,
          ip_count = ip
        )
    }

    println("count:" + toResult.count())
    clearReportHourData("report_media_ip_imei", date1)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_media_ip_imei", mariadbProp)
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
  case class ImeiDiff(
                       date: String = "",
                       media_id:Int = 0,
                       adslot_id:Int = 0,
                       adslot_type:Int = 0,
                       imei_count:Int = 0,
                       ip_count:Int = 0
                     )


}