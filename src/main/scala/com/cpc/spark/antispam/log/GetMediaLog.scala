package com.cpc.spark.antispam.log

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetMediaLog {
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
    val dayBefore = args(0).toInt
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("media request anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var sql1 = ("SELECT *," +
      "ext['os_version'].string_value as os_version," +
      "ext['client_type'].string_value as client_type ," +
      "ext['client_version'].string_value as client_version  ," +
      "ext['device_ids'].string_value as device_ids " +
      "from dl_cpc.cpc_union_log where `date`='%s' and isfill =1  ").format(date)

    println("sql1:" + sql1)
    var rddData = ctx.sql(sql1)
      //      .as[UnionLog]
      .rdd.cache()

    var allData = rddData.map(x => ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type")),1)).reduceByKey((x, y) => x + y)
      .map{
        case ((media_appsid, adslotid, adslot_type2),count2) =>
          var media = MediaRequest(
            date = date,
            media_id = media_appsid.toInt,
            adslot_id = adslotid.toInt,
            adslot_type = adslot_type2,
            count = count2
          )
          (media.key, media)
      }

    val mediaTypeRdd =  rddData.map(x => ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"),x.getAs[Int]("media_type")),1)).reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,media_type), num) =>
        ((media_appsid,adslotid,adslot_type),""+ media_type+","+ num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type),value) =>
        val media = MediaRequest(date, media_appsid.toInt ,adslotid.toInt ,adslot_type, 0,value)
        (media.key, media)
    }
    allData = allData.leftOuterJoin(mediaTypeRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          media_type= other2.media_type
        )
        (key,media2)
    }
    val userAgentRdd =  rddData.map{
      case x =>
        var isExitUserAgent = 0
        if(x.getAs[String]("ua").length>3){
          isExitUserAgent =1
        }
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"),isExitUserAgent),1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,isExitUserAgent), num) =>
        ((media_appsid,adslotid,adslot_type),isExitUserAgent +","+ num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          user_agent = value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(userAgentRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          user_agent= other2.user_agent
        )
        (key,media2)
    }
    val osTypeRdd =  rddData.map(x => ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"),x.getAs[Int]("os")),1)).reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,os_type), num) =>
        ((media_appsid,adslotid,adslot_type),os_type+","+num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          os_type = value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(osTypeRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 =  media.copy(
          os_type= other2.os_type
        )
        (key,media2)
    }
    val osVersionRdd =  rddData.map{
      case x =>
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[String]("os_version")),1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,version), num) =>
        ((media_appsid,adslotid,adslot_type),version+"," +num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          os_version = value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(osVersionRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          os_version= other2.os_version
        )
        (key,media2)
    }
    val osUaRdd =  rddData.map{
      case x =>
        var deviceType = "no"
        val imeiRegex = """^[0-9]*$""".r
        val idfaRegex = """^([0-9a-zA-Z]{1,})(([/\s-][0-9a-zA-Z]{1,}){4})$""".r
        if(x.getAs[String]("uid") == x.getAs[String]("ip")){
          deviceType = "ip"
        }else if(!imeiRegex.findFirstMatchIn(x.getAs[String]("uid")).isEmpty){
          deviceType = "imei"
        }else if(!idfaRegex.findFirstMatchIn(x.getAs[String]("uid")).isEmpty){
          deviceType = "imei"
        }
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), deviceType),1)

    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,deviceType), num) =>
        ((media_appsid,adslotid,adslot_type),deviceType +","+num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          device_type= value
        )
        (media.key, media)
    }

    allData = allData.leftOuterJoin(osUaRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 =  media.copy(
          device_type= other2.device_type
        )
        (key,media2)
    }

    val clientTypeRdd =  rddData.map{
      case x =>
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[String]("client_type")),1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,client_type), num) =>
        ((media_appsid,adslotid,adslot_type),client_type +","+ num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          client_type= value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(clientTypeRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          client_type= other2.client_type
        )
        (key,media2)
    }

    val clientVersionRdd =  rddData.map{
      case x =>
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[String]("client_version")),1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,client_version), num) =>
        ((media_appsid,adslotid,adslot_type),client_version+ ","+ num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          client_version= value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(clientVersionRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 =  media.copy(
          client_version= other2.client_version
        )
        (key,media2)
    }


    val networkTypeRdd =  rddData.map{
      case x =>
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[Int]("network")),1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,network), num) =>
        ((media_appsid,adslotid,adslot_type),network+","+num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          network_type= value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(networkTypeRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          network_type= other2.network_type
        )
        (key,media2)
    }
    val networkIpRdd =  rddData.map{
      case x =>
        ((x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[Int]("network"),x.getAs[String]("ip")),1)
    }.reduceByKey((x, y) => x).map{
      case ((media_appsid, adslotid, adslot_type, network, ip), num) =>
        ((media_appsid, adslotid, adslot_type, network), 1)
    }.reduceByKey((x, y) => x + y).map{
      case ((media_appsid,adslotid,adslot_type,network), num) =>
        ((media_appsid,adslotid,adslot_type),network + "," + num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          network_ip= value
        )
        (media.key, media)
    }
    allData = allData.leftOuterJoin(networkIpRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          network_ip= other2.network_ip
        )
        (key,media2)
    }

    val deviceIdsRdd =  rddData.map{
      case x =>
        (x.getAs[String]("media_appsid"),x.getAs[String]("adslotid"),x.getAs[Int]("adslot_type"), x.getAs[String]("device_ids"))
    }.filter(x => x._4.length() > 0 ).map{
      case (media_appsid,adslotid,adslot_type,device_ids) =>
        var deviceIdsArr = device_ids.split(";")
        var list = Seq[(String,String,Int,String)]()
        deviceIdsArr.foreach{
          x =>
            list = list :+ (media_appsid,adslotid,adslot_type,x.split(":")(0))
        }
        list
    }.flatMap(x => x).filter(x => x._4.length > 0).map{
      case (media_appsid,adslotid,adslot_type,device_id) =>
        ((media_appsid,adslotid,adslot_type,device_id), 1)
    }.reduceByKey((x, y) => x+y).map{
      case ((media_appsid,adslotid,adslot_type,device_id), num) =>
        ((media_appsid,adslotid,adslot_type),device_id + "," + num)
    }.reduceByKey((x, y) => x +";"+ y) .map{
      case ((media_appsid,adslotid,adslot_type2),value) =>
        val media = MediaRequest(
          date = date,
          media_id = media_appsid.toInt,
          adslot_id = adslotid.toInt,
          adslot_type = adslot_type2,
          device_ids= value
        )
        (media.key, media)
    }

    allData = allData.leftOuterJoin(deviceIdsRdd).map{
      case (key, (media, other:Option[MediaRequest]))=>
        var other2 = other.getOrElse(MediaRequest())
        var  media2 = media.copy(
          device_ids= other2.device_ids
        )
        (key,media2)
    }

    val toResult = allData.map(x => x._2)
    println("count:" + toResult.count())
    clearReportHourData("report_media_request_info", date)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_media_request_info", mariadbProp)
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

  case class MediaRequest(
                           date: String = "",
                           media_id:Int = 0,
                           adslot_id:Int = 0,
                           adslot_type:Int = 0,
                           count: Int = 0,
                           media_type: String = "",
                           user_agent: String = "",
                           os_type: String = "",
                           os_version: String = "",
                           device_type: String = "",
                           client_type: String = "",
                           client_version: String = "",
                           network_type: String = "",
                           network_ip: String = "",
                           device_ids: String = ""
                         ){
    val key = "%d-%d-%d".format(media_id, adslot_id, adslot_type)
  }

}