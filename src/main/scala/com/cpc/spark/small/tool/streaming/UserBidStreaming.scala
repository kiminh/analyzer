package com.cpc.spark.small.tool.streaming

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.LogData
import com.cpc.spark.log.parser.LogParser
import com.cpc.spark.small.tool.streaming.tool.OffsetRedis
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by wanli on 2018/10/10.
  */
object UserBidStreaming {

  /**
    * 创建一个offsetRedis对象
    * 调用方法设置Redis的key前缀
    */
  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("SMALL_TOOL_USERBID_KAFKA_OFFSET")

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    var spark = SparkSession
      .builder()
      .appName("small.tool.streaming UserBidStreaming")
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val topicsSet = "cpc_search_new".split(",").toSet
    val brokers = "192.168.80.35:9092,192.168.80.36:9092,192.168.80.37:9092,192.168.80.88:9092,192.168.80.89:9092"
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    var conf = ConfigFactory.load()

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
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      println("no offset")
    } else {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
      println("from offset")
    }


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

    val base_data = messages.repartition(1000).map {
      case (k, v) =>
        try {
          val logdata = new LogData(v)
          val log_timestamp = logdata.log.getLogTimestamp
          val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
          val hour = new SimpleDateFormat("HH").format(log_timestamp)
          val minute = new SimpleDateFormat("mm").format(log_timestamp).charAt(0) + "0"

          val field2 = scala.collection.mutable.Map[String, ExtValue]()

          val fieldCount = logdata.log.getField.getMapCount
          for (i <- 0 until fieldCount) {
            val field = logdata.log.getField.getMap(i)
            val extValue = ExtValue(field.getValue.getIntType, field.getValue.getLongType, field.getValue.getFloatType, field.getValue.getStringType)
            field2 += (field.getKey -> extValue)

          }

          val field: collection.Map[String, ExtValue] = field2.toMap
          SrcLog(logdata.log.getLogTimestamp, logdata.log.getIp, field, date, hour, minute)

        } catch {
          case t: Throwable =>
            t.printStackTrace() // TODO: handle error
            null
        }
    }
      .filter(_ != null)

    base_data.print()

    var date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)

    base_data.foreachRDD {
      rs =>
        val keys = rs.map {
          x =>
            (x.thedate, x.thehour, x.theminute)
        }
          .distinct().toLocalIterator


        keys.foreach { //(日期，小时)
          key =>
            val part = rs.filter(r => r.thedate == key._1 && r.thehour == key._2 && r.theminute == key._3)
            val numbs = part.take(1).length
            if (numbs > 0) {
              println("")
              part.take(1).foreach(println)
              println("")
            }

          //            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
          //            println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))
          //
          //            if (numbs > 0) {
          //
          //            }
        }
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
