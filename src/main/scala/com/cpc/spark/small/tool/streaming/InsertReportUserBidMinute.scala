package com.cpc.spark.small.tool.streaming

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.cpc.spark.common.LogData
import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.cpc.spark.small.tool.streaming.tool.OffsetRedis
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanli on 2018/10/10.
  */
object InsertReportUserBidMinute {

  /**
    * 创建一个offsetRedis对象
    * 调用方法设置Redis的key前缀
    */
  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("SMALL_TOOL_InsertReportUserBidMinute_KAFKA_OFFSET")

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()


  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: small.tool.streaming.InsertReportUserBidMinute <brokers> <topics> <seconds>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <seconds> is execute time Seconds
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, seconds) = args

    println("brokers:", brokers, "topics:", topics, "seconds:", seconds)

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    var spark = SparkSession
      .builder()
      .appName("small.tool.streaming InsertReportUserBidMinute")
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

    if (kafkaoffset == null || kafkaoffset.isEmpty) {
      //if (true) {
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
            field
          } catch {
            case t: Throwable =>
              t.printStackTrace() // TODO: handle error
              null
          }
      }
      .filter(_ != null)
      .map {
        x =>
          val s1 = x.getOrElse[ExtValue]("cpc_search_new", null)
          if (s1 == null) {
            null
          } else {
            s1.string_type
          }
      }
      .filter(_ != null)

    val allData = base_data
      .map(x => LogParser.parseSearchLog(x))
      .filter { x =>
        (x != null) && (x.isfill > 0) && (x.adsrc == 1) &&
          ((x.media_appsid == "80000001") || (x.media_appsid == "80000002") || (x.media_appsid == "80000006") ||
            (x.media_appsid == "800000062") || (x.media_appsid == "80000064") || (x.media_appsid == "80000066") || (x.media_appsid == "80000141"))
      }
      .map {
        x =>
          val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm")
          val timeStr = fm.format(new Date(x.timestamp.toLong * 1000)) + ":00"

          val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date(x.timestamp.toLong * 1000))

          var cvrThresholdByUser = 0.toLong
          if (x.ext_int.contains("cvr_threshold_by_user")) {
            cvrThresholdByUser = x.ext_int("cvr_threshold_by_user")
          }

          var cvrRealBid = 0
          if (x.ext.contains("cvr_real_bid")) {
            cvrRealBid = x.ext("cvr_real_bid").int_value
          }

          var cvrType = "nocvr"
          if (cvrThresholdByUser == 200) {
            cvrType = "cvr2"
          } else if (cvrThresholdByUser > 0) {
            cvrType = "cvr1"
          }

          var userId = x.userid

          ((timeStr, x.adslot_type, cvrType, userId), (1.toLong, x.bid.toLong, dateStr, cvrRealBid.toLong))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2, a._3, a._4 + b._4)
      }
      .map {
        x =>
          val dateTime = x._1._1
          val adslotType = x._1._2
          val cvrType = x._1._3
          val userId = x._1._4
          val isfill = x._2._1
          val bid = x._2._2
          val dateStr = x._2._3
          val cvrRealBid = x._2._4
          (dateTime, userId, adslotType, cvrType, isfill, bid, cvrRealBid, dateStr)
      }
      .foreachRDD {
        rdd =>
          val insertDataFrame = spark
            .createDataFrame(rdd)
            .toDF("create_time", "user_id", "adslot_type", "cvr_type", "served_request", "sum_bid", "sum_cvr_real_bid", "date")
          insertDataFrame.show(5)

          insertDataFrame
            .write
            .mode(SaveMode.Append)
            .jdbc(mariaReport2dbUrl, "report2.report_user_bid_minute", mariaReport2dbProp)

          val now: Date = new Date()
          val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val date = dateFormat.format(now)
          println("----------------------------", date, insertDataFrame.count())

      }

    ssc.start()
    ssc.awaitTermination()
    ///////////////////--------------------------
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
