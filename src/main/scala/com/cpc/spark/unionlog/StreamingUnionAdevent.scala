package com.cpc.spark.unionlog

import java.text.SimpleDateFormat
import java.util.Date

import adevent.adevent
import com.cpc.spark.streaming.anal.CpcStreamingSearchLogParser.currentBatchStartTime
import com.cpc.spark.streaming.tools.OffsetRedis
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}


/**
  * Created on ${Date} ${Time}
  */
object StreamingUnionAdevent {

  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("FLINK_UNION_ADEVENT_KAFKA_OFFSET")

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: CpcStreamingAnal <brokers> <topics> <out_topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <outTable> is the out table
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, seconds, outTable) = args

    val sparkConf = new SparkConf().setAppName("topic:flink_adevent_union = " + topics)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))

    val topicsSet = topics.split(",").toSet
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
      kafkaoffset.foreach(tp => {
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
      })
    }
    println("***************last offsets*******************")
    println(kafkaoffset)


    var messages: InputDStream[(String, Array[Byte])] = null

    if (kafkaoffset == null || kafkaoffset.isEmpty) {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      println("no offset")
    } else {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
      println("from offset")
    }

    messages.foreachRDD {
      rdd => {
        currentBatchStartTime = new Date().getTime //每个batch的开始时间

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
    getCurrentDate("end-offsetRanges")

    val base_data = messages.map {
      case (k, v) =>
        try {
          val event = adevent.AdActionEvent.parseFrom(v)
          val log_timestamp = event.timestamp
          val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
          val hour = new SimpleDateFormat("HH").format(log_timestamp)
          val minute = new SimpleDateFormat("mm").format(log_timestamp).charAt(0) + "0"

          val alog = AdeventUnionLog(
            timestamp = event.timestamp,
            insertionID = event.insertionID,
            searchid = event.searchID,
            isclick = event.isClick,
            isfill = event.isFill,
            isshow = event.isShow,
            userID = event.getAd.userid,
            thedate = date,
            thehour = hour,
            theminute = minute
          )

          alog

        } catch {
          case t: Throwable =>
            t.printStackTrace()
            null
        }
    }.filter(_ != null)

    base_data.print(5)

    var date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)

    base_data.foreachRDD {
      rs =>
        val keys = rs.map { x => (x.thedate, x.thehour, x.theminute) }.distinct().toLocalIterator
        val spark = SparkSession.builder().config(ssc.sparkContext.getConf).enableHiveSupport().getOrCreate()

        keys.foreach {
          key =>
            val part = rs.filter(r => r.thedate == key._1 && r.thehour == key._2 && r.theminute == key._3)
            val numbs = part.count()
            val clickOutFiles = (numbs / 1000000 + 1).toInt //用于动态减少search输出文件数；logparsed_cpc_search_minute 2018-10-09 19 00大约50w数据，大小40m,预设1个文件1g

            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
            println("~~~~~~~~~ fm_click_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))

            if (numbs > 0) {
              //fm_click持久化
              val df = spark.createDataFrame(part)
                .repartition(clickOutFiles)
                .write
                .mode(SaveMode.Append)
                .parquet("/warehouse/test.db/%s/%s/%s/%s".format(outTable, key._1, key._2, key._3))

              val sqlStmt =
                """
                  |ALTER TABLE test.%s add if not exists PARTITION (thedate = "%s", thehour = "%s", theminute = "%s")  LOCATION
                  |       '/warehouse/test.db/%s/%s/%s/%s'
                  |
                    """.stripMargin.format(outTable, key._1, key._2, key._3, outTable, key._1, key._2, key._3)
              println(sqlStmt)
              spark.sql(sqlStmt)
            }

        }

      /**
        * 报警日志写入kafka的topic: cpc_realtime_parsedlog_warning
        */
      // 每个batch的结束时间
      /*
      val currentBatchEndTime = new Date().getTime
      val costTime = (currentBatchEndTime - currentBatchStartTime) / 1000.0

      val mapString: Seq[(String, String)] = Seq(("Topic", "fm_click"))
      val mapFloat: Seq[(String, Float)] = Seq(("ProcessingTime", costTime.toFloat))
      val data2Kafka = new Data2Kafka()
      data2Kafka.setMessage(currentBatchEndTime, null, mapFloat, null, mapString)
      data2Kafka.sendMessage(brokers, "cpc_fm_warning")
      data2Kafka.close()
      */
    }


    ssc.start()
    ssc.awaitTermination()

  }


  def getKafkaTopicAndPartitionOffset(topicsSet: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    var kafkaCluster = new KafkaCluster(kafkaParams)
    var topicAndPartitions = Set[TopicAndPartition]()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val partitions: Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicsSet)
    partitions match {
      case Left(x) => System.err.println("kafka getPartitions error" + x);
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
      case Left(x) => System.err.println("kafka getConsumerOffsets error" + x);
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

  def getCurrentDate(message: String) {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format(now)
    if (message.length() > 0) {
      println("currentDate:" + message + ":" + hehe)
    } else {
      print("currentDate:" + hehe)
    }
  }

  case class AdeventUnionLog(
                         var timestamp: Long = 0,
                         var searchid: String = "",
                         var insertionID: String = "", // unique key for each ad insertion
                         var requestID: String = "",
                         var userID: Int = 0,
                         var isfill: Int = 0,
                         var isshow: Int = 0,
                         var isclick: Int = 0,
                         var thedate: String = "",
                         var thehour: String = "",
                         var theminute: String = ""
                       )

}
