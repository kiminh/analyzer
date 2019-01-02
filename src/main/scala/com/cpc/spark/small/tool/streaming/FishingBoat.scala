package com.cpc.spark.small.tool.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import util.control.Breaks._

import com.cpc.spark.common.LogData
import com.cpc.spark.small.tool.streaming.parser.LogParser
import com.cpc.spark.small.tool.streaming.tool.OffsetRedis
import com.fasterxml.jackson.annotation.JsonTypeInfo.None
import com.google.gson.{Gson, JsonElement, JsonParser}
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanli on 2018/10/10.
  */
object FishingBoat {

  case class Info(
                   task_id: Int = 0,
                   task_key: String = "",
                   request: Long = 0,
                   served_request: Long = 0,
                   impression: Long = 0,
                   click: Long = 0,
                   cost: Long = 0,
                   device_total: Long = 0,
                   date: String = "",
                   create_time: String = ""
                 ) {

  }


  /**
    * 创建一个offsetRedis对象
    * 调用方法设置Redis的key前缀
    */
  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("SMALL_TOOL_FishingBoat_KAFKA_OFFSET")

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()


  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: small.tool.streaming.FishingBoat <brokers> <topics> <seconds>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <seconds> is execute time Seconds
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, seconds, taskNum) = args

    println("brokers:", brokers, "topics:", topics, "seconds:", seconds, "taskNum:", taskNum)


    //    var tmpMap :scala.collection.mutable.Map[String, Object] = scala.collection.mutable.Map()
    //    tmpMap += ("adslot_type"->"1")
    //    tmpMap += ("adslotid"->"1029077")
    //    var confJsonStr =
    //      """
    //        |{
    //        |	"filter": [{
    //        |		"field": "adslot_type",
    //        |		"mark": "in",
    //        |		"value": "1,2"
    //        |	}, {
    //        |		"field": "adslotid",
    //        |		"mark": ">",
    //        |		"value": "1029076"
    //        |	}, {
    //        |		"field": "adslotid",
    //        |		"mark": "<",
    //        |		"value": "1029078"
    //        |	}],
    //        |	"group": "network,sex",
    //        |	"group_filter": {
    //        |		"field": "isfill",
    //        |		"mark": ">",
    //        |		"total": "10000"
    //        |	}
    //        |}
    //      """.stripMargin
    //    println("getFilter",getFilter(tmpMap,confJsonStr))
    //
    //    return

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    var spark = SparkSession
      .builder()
      .appName("small.tool.streaming FishingBoat")
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    //"cpc_search_new"
    //val brokers = "192.168.80.35:9092,192.168.80.36:9092,192.168.80.37:9092,192.168.80.88:9092,192.168.80.89:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())

    try {
      for (topic <- topicsSet) {
        val partitions = offsetRedis.getPartitionByTopic(topic)
        for (partition <- partitions) {
          println("topic:" + topic + ";partition:" + partition)
          val tp = TopicAndPartition(topic, partition.toInt)
          fromOffsets += (tp -> offsetRedis.getTopicAndPartitionOffSet(topic, partition.toInt))
        }
      }
    } catch {
      case t: Exception => System.err.println("connect redis exception" + t.getStackTrace)
        fromOffsets = Map()
    }

    println("***************redis fromOffsets*******************")
    println(fromOffsets)

    var kafkaoffset: Map[TopicAndPartition, Long] = null

    if (fromOffsets.nonEmpty) {
      kafkaoffset = getKafkaTopicAndPartitionOffset(topicsSet, kafkaParams)

      println("***************from kafka offsets*******************")
      println(kafkaoffset)

      kafkaoffset.foreach(
        tp => {
          try {
            //可能key不存在
            if (fromOffsets.contains(tp._1)) {
              if (fromOffsets(tp._1) < tp._2) {
                kafkaoffset += (tp._1 -> fromOffsets(tp._1))

              }
            }

          } catch {
            case t: Throwable =>
              t.printStackTrace()
              println("set kafkaoffset key filled" + t.printStackTrace()) // TODO: handle error
          }
        }
      )
    }
    println("***************last offsets*******************")
    println(kafkaoffset)

    var messages: InputDStream[(String, Array[Byte])] = null

    //if (kafkaoffset == null || kafkaoffset.isEmpty) {
    if (true) {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      println("no offset")
    } else {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
      println("from offset")
    }

    //messages.print()

    messages.foreachRDD {
      rdd => {
        //每个batch的开始时间
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { iter =>
          try {
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            offsetRedis.setTopicAndPartitionOffSet(o.topic, o.partition, o.fromOffset)
          } catch {
            case t: Throwable =>
              t.printStackTrace()
              println("connect redis exception in offsetRanges " + t.getStackTrace)
          }

        }
      }
    }

    var broadcastTaskList = spark.sparkContext.broadcast(Map[Int, (Int, String)]())

    val base_data = messages
      .map {
        case (k, v) =>
          try {
            val logdata = new LogData(v)
            val field2 = scala.collection.mutable.Map[String, ExtValue]()
            val fieldCount = logdata.log.getField.getMapCount

            for (i <- 0 until fieldCount) {
              val field = logdata.log.getField.getMap(i)
              val extValue = ExtValue(field.getValue.getIntType, field.getValue.getLongType, field.getValue.getFloatType, field.getValue.getStringType)
              field2 += (field.getKey -> extValue)
            }

            val field: collection.Map[String, ExtValue] = field2.toMap
            SrcLog(logdata.log.getLogTimestamp, logdata.log.getIp, field)
          } catch {
            case t: Throwable =>
              t.printStackTrace() // TODO: handle error
              null
          }
      }
      .filter(_ != null)
      .map {
        x =>
          var data = ("", "")
          val s1 = x.field.getOrElse[ExtValue]("cpc_search_new", null) //cpc_search_new
          if (s1 == null) {
            null
          } else {
            data = ("cpc_search_new", s1.string_type)
          }

          val s2 = x.field.getOrElse[ExtValue]("cpc_show_new", null) //cpc_search_new
          if (s2 == null) {
            null
          } else {
            data = ("cpc_show_new", x.log_timestamp.toString + s2.string_type)
          }

          val s3 = x.field.getOrElse[ExtValue]("cpc_click_new", null) //cpc_search_new
          if (s3 == null) {
            null
          } else {
            data = ("cpc_click_new", s3.string_type)
          }

          if (data == ("", "")) null else data

      }
      .filter(_ != null)

    val allData = base_data
      .map { x =>
        if (x._1 == "cpc_search_new") {
          LogParser.parseSearchLog(x._2)
        } else if (x._1 == "cpc_show_new") {
          LogParser.parseShowLog(x._2)
        } else {
          LogParser.parseClickLog2(x._2)
        }
      }
      .filter {
        x =>
          x != null && (x.ext.contains("charge_type") && x.ext.get("charge_type").get.int_value == 1) && x.adsrc == 1
      }
      .repartition(50)
      .map {
        x =>
          x.isclick = x.isCharged()
          x.isfill = if (x.isclick == 1) 0 else x.isfill
          x.price = if (x.isclick == 1) x.price else 0
          val xx = x.getClass
          val xxf = xx.getDeclaredFields


          var infoMap = scala.collection.mutable.Map[String, Object]()

          val client_type = if (x.ext.contains("client_type")) x.ext.get("client_type").get.string_value else ""
          infoMap += ("client_type" -> client_type)

          val clientVersion = if (x.ext.contains("client_version")) x.ext.get("client_version").get.string_value else ""
          infoMap += ("client_version" -> clientVersion)

          val usertype = if (x.ext.contains("usertype")) x.ext.get("usertype").get.int_value else 0
          infoMap += ("usertype" -> usertype.toString)

          val exp_style = if (x.ext_int.contains("exp_style")) x.ext_int.get("exp_style").get else 0.toLong
          infoMap += ("exp_style" -> exp_style.toString)

          val hostname = if (x.ext_string.contains("hostname")) x.ext_string.get("hostname").get else ""
          infoMap += ("hostname" -> hostname.toString)

          if (x.exptags.length > 0) {
            val tmpArr = x.exptags.split(",")
            for (tval <- tmpArr) {

              if (tval.contains("=")) {
                val tmpInfo = tval.split("=")
                if (tmpInfo.length >= 2) {
                  infoMap += (tmpInfo(0) -> tmpInfo(1))
                } else {
                  infoMap += (tval -> tval)
                }

              } else if (tval.contains(":")) {

                val tmpInfo = tval.split(":")
                if (tmpInfo.length >= 2) {
                  infoMap += (tmpInfo(0) -> tmpInfo(1))
                } else {
                  infoMap += (tval -> tval)
                }

              } else {
                infoMap += (tval -> tval)
              }
            }
          }


          xxf.map {
            y =>
              y.setAccessible(true)
              infoMap += (y.getName -> y.get(x))
          }

          infoMap
      }
      .cache()

    //        allData.filter(x=>x.get("adslotid").get.toString=="").foreachRDD(rdd => rdd.take(10).foreach(println))
    //
    //        println("--------------")


    for (i <- 1 to taskNum.toInt) {

      val baseAllData = allData
        .filter {
          x =>
            x != null && broadcastTaskList.value.contains(i) && getFilter(x, broadcastTaskList.value.get(i).get._2)
        }
        .cache()

      val deviceAllData = baseAllData
        //.filter(_.get("isshow").get.toString.toLong > 0)
        .map {
        x =>
          val info = broadcastTaskList.value.get(i).get
          val confJson = info._2
          val isshow = x.get("isshow").get.toString.toLong
          ((getKey(x, confJson), x.get("uid").get), (1))
      }
        .filter(_._1._1.size > 0)
        .reduceByKey {
          (a, b) =>
            a
        }
        .map {
          x =>
            ((x._1._1), 1.toLong)
        }
        .reduceByKey {
          (a, b) =>
            (a + b)
        }
        .map {
          x =>
            val info = broadcastTaskList.value.get(i).get
            ((x._1), (Info(info._1, "", 0, 0, 0, 0, 0, x._2)))
        }


      baseAllData
        .map {
          x =>
            val info = broadcastTaskList.value.get(i).get
            val confJson = info._2
            val isfill = 0.toLong
            //x.get("isfill").get.toString.toLong
            val isshow = x.get("isshow").get.toString.toLong
            val isclick = x.get("isclick").get.toString.toLong
            val price = x.get("price").get.toString.toLong
            //val bid = x.get("bid").get.toString.toLong
            ((getKey(x, confJson)), (Info(info._1, "", 0, isfill, isshow, isclick, price)))
        }
        .union(deviceAllData)
        .reduceByKey {
          (a, b) =>
            (Info(a.task_id, a.task_key, a.request + b.request, a.served_request + b.served_request, a.impression + b.impression, a.click + b.click, a.cost + b.cost, a.device_total + b.device_total))
        }
        .filter {
          x =>
            val confJson = broadcastTaskList.value.get(i).get._2
            x != null && getGroupFilter(x._2, confJson) && x._1.size > 0
        }
        .map {
          x =>
            val info = x._2
            val now: Date = new Date()
            val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")
            val date = dateFormat.format(now).substring(0, 10)
            val dateTimeStr = dateFormat.format(now) + ":00"
            val gson = new Gson
            val task_key = gson.toJson(x._1)
            (info.task_id, task_key, info.served_request, info.served_request, info.impression, info.click, info.cost, info.device_total, date, dateTimeStr)
        }

        .foreachRDD {
          rdd =>
            //rdd.take(5).foreach(println)
            val insertDataFrame = spark
              .createDataFrame(rdd)
              .toDF("task_id", "task_key", "request", "served_request", "impression", "click", "cost", "device_total", "date", "create_time") //, "date", "create_time"
            insertDataFrame.show(5)

            insertDataFrame
              .write
              .mode(SaveMode.Append)
              .jdbc(mariaReport2dbUrl, "report2.fishing_boat_task_warehouse", mariaReport2dbProp)

            println("over~", i)
        }
    }

    allData
      .filter(x => false)
      .foreachRDD {
        rdd =>
          var taskList = spark.read.jdbc(mariaReport2dbUrl,
            """
              |(
              | SELECT id,task_config
              | FROM fishing_boat_task
              | WHERE task_status=0 AND start_time<=NOW() AND end_time>NOW()
              | LIMIT %d
              |) xx
            """.stripMargin.format(taskNum.toInt), mariaReport2dbProp)
            .rdd
            .map {
              x =>
                var id = x.get(0).toString.toInt
                var taskConfig = x.get(1).toString
                (id, taskConfig)
            }
            .take(99999)
          println("taskList", taskList)

          broadcastTaskList.unpersist()

          var taskMaps: Map[Int, (Int, String)] = Map()
          var ti: Int = 0
          for (tinfo <- taskList) {
            ti += 1
            println(ti, tinfo)
            taskMaps += (ti -> tinfo)
          }

          broadcastTaskList = spark.sparkContext.broadcast(taskMaps)
      }


    ssc.start()
    ssc.awaitTermination()
    ///////////////////--------------------------
  }

  def getKey(info: scala.collection.mutable.Map[String, Object], confJsonStr: String): Map[String, Object] = {
    var keyMap: Map[String, Object] = Map()
    //    var confJsonStr =
    //      """
    //        |{
    //        |	"group": "network,sex",
    //        |	"filter": {
    //        |		"field": "isfill",
    //        |		"mark": ">",
    //        |		"total": "10000"
    //        |	}
    //        |}
    //      """.stripMargin
    val parser = new JsonParser()
    val jsonObj = parser.parse(confJsonStr).getAsJsonObject

    if (!jsonObj.has("group")) {
      return keyMap
    }

    val gNameArr = jsonObj.get("group").getAsString.split(",")

    for (gval <- gNameArr) {
      if (info.contains(gval)) {
        keyMap += (gval -> info.get(gval).get)
      }
    }

    if (keyMap.size == 0 && jsonObj.has("is_group_contrast") && jsonObj.get("is_group_contrast").getAsString == "1") {
      keyMap += ("group_contrast" -> "group_contrast")
    }

    keyMap
  }

  def getGroupFilter(info: Info, confJsonStr: String): Boolean = {
    var keyMap: Map[String, String] = Map()
    //        var confJsonStr1 =
    //          """
    //            |{
    //            |	"group": "network,sex",
    //            |	"filter": {
    //            |		"field": "isfill",
    //            |		"mark": ">",
    //            |		"total": "10000"
    //            |	}
    //            |}
    //          """.stripMargin
    val parser = new JsonParser()
    val jsonObj = parser.parse(confJsonStr).getAsJsonObject

    var isOk = true

    if (!jsonObj.has("group_filter")) {
      return isOk
    }

    val filterObj = jsonObj.get("group_filter").getAsJsonObject

    if (!filterObj.has("field") || !filterObj.has("mark") || !filterObj.has("total")) {
      return isOk
    }

    val fieldStr = filterObj.get("field").getAsString
    val markStr = filterObj.get("mark").getAsString
    val total = filterObj.get("total").getAsLong

    var infoMap: Map[String, Long] = Map()

    infoMap += ("fill" -> info.served_request)
    infoMap += ("show" -> info.impression)
    infoMap += ("click" -> info.click)
    infoMap += ("price" -> info.cost)


    val fieldValue = infoMap.get(fieldStr).get


    if (markStr == "<") {
      isOk = fieldValue < total
    } else if (markStr == "=") {
      isOk = fieldValue == total
    } else if (markStr == ">") {
      isOk = fieldValue > total
    }
    isOk
  }

  def getFilter(info: scala.collection.mutable.Map[String, Object], confJsonStr: String): Boolean = {
    var keyMap: Map[String, String] = Map()

    val parser = new JsonParser()
    val jsonObj = parser.parse(confJsonStr).getAsJsonObject

    if (!jsonObj.has("filter")) {
      return true
    }

    val filterJArr = jsonObj.get("filter").getAsJsonArray


    var status: Int = 0

    if (filterJArr.size > 0) {
      for (i <- 0 to (filterJArr.size - 1)) {
        val finfo = filterJArr.get(i).getAsJsonObject

        val tmpField = finfo.get("field").getAsString
        val tmpMark = finfo.get("mark").getAsString
        val tmpVal = finfo.get("value").getAsString

        if (info.contains(tmpField)) {
          val infoVal = info.get(tmpField).get.toString

          if (tmpMark == "in") {
            breakable(
              for (tmpv <- tmpVal.split(",")) {
                if (infoVal == tmpv) {
                  status += 1
                  break()
                }
              }
            )

          } else if (tmpMark == "not in") {
            breakable(
              for (tmpv <- tmpVal.split(",")) {
                if (infoVal != tmpv) {
                  status += 1
                  break()
                }
              }
            )

          } else if (tmpMark == ">") {
            if (infoVal > tmpVal) {
              status += 1
            }

          } else if (tmpMark == "<") {
            if (infoVal < tmpVal) {
              status += 1
            }

          }
        }
      }
    }

    return if (status == 0 || filterJArr.size > status) false else true
  }

  def getKafkaTopicAndPartitionOffset(topicsSet: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    var kafkaCluster = new KafkaCluster(kafkaParams)
    var topicAndPartitions = Set[TopicAndPartition]()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val partitions: Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicsSet)
    partitions match {
      case Left(x) => System.err.println("kafka getPartitions error" + x)
        System.exit(1)
      case Right(x) =>
        for (partition <- x) {
          topicAndPartitions += partition
        }
    }
    println("***************from kafka TopicAndPartition*******************")
    println(topicAndPartitions)
    val consumerOffset: Either[KafkaCluster.Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
    consumerOffset match {
      case Left(x) => System.err.println("kafka getConsumerOffsets error" + x)
        System.exit(1)
      case Right(x) =>
        x.foreach(
          tp => {
            fromOffsets += (tp._1 -> tp._2.offset)
          }
        )
    }
    fromOffsets
  }

  case class SrcLog(
                     log_timestamp: Long = 0,
                     ip: String = "",
                     field: collection.Map[String, ExtValue] = null,
                     thedate: String = "",
                     thehour: String = "",
                     theminute: String = ""
                   )

  case class ExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")

  /////////////////////--------------------------
}
