package com.cpc.spark.streaming.anal

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, TaskContext, rdd}
import com.cpc.spark.common.LogData
import org.apache.spark.streaming.dstream.InputDStream
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.log.anal.MergeLog.{getDateHourPath, srcRoot}
import com.cpc.spark.log.parser.{LogParser, UnionLog}
import com.cpc.spark.streaming.tools.OffsetRedis
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset

object CpcStreamingLogParser3 {

  /**
    * 创建一个offsetRedis对象
    * 调用方法设置Redis的key
    */
  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("PARSED_LOG_KAFKA_OFFSET")

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: CpcStreamingAnal <brokers> <topics> <out_topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <out_topics> is the out to kafka topic
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, out_topics, seconds) = args

    val sparkConf = new SparkConf().setAppName("srcLog: topics = " + topics)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)


    val conf = ConfigFactory.load()


    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
    try {
      for (topic <- topicsSet) {
        //        val partitions = redis.smembers[String](topic).get
        val partitions = offsetRedis.getPartitionByTopic(topic)
        for (partition <- partitions) {
          println("topic:" + topic + ";partition:" + partition)
          val tp = TopicAndPartition(topic, partition.toInt)
          fromOffsets += (tp -> offsetRedis.getTopicAndPartitionOffSet(topic, partition.toInt))
        }
      }
      // OffsetRedis.refreshOffsetKey()
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

//    if (kafkaoffset == null || kafkaoffset.isEmpty) {
    if (true) {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      println("no offset")
    } else {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
      println("from offset")
    }

    messages.foreachRDD {
      rdd => {
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
          val logdata = LogData.parseData(v)
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
      .filter(_ != null).repartition(500)

    base_data.print()

    val spark = SparkSession.builder()
      .appName("zyc_kafka_test")
      .enableHiveSupport()
      .getOrCreate()


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

            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
            println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))

            if (numbs > 0) {

              //日志解析
              val parserLogData = getParsedLog(part, topics.split(",")(0))

              //配置修改
              val table = conf.getString("topic2tbl_logparsed." + topics.split(",")(0))

              spark.createDataFrame(parserLogData)
                .write
                .mode(SaveMode.Append)
                .parquet("/warehouse/dl_cpc.db/%s/%s/%s/%s".format(table, key._1, key._2, key._3))


              val sqlStmt =
                """
                  |ALTER TABLE dl_cpc.%s add if not exists PARTITION (thedate = "%s", thehour = "%s", theminute = "%s")  LOCATION
                  |       '/warehouse/dl_cpc.db/%s/%s/%s/%s'
                  |
                """.stripMargin.format(table, key._1, key._2, key._3, table, key._1, key._2, key._3)
              println(sqlStmt)
              spark.sql(sqlStmt)

            }
        }
    }
    //    if (allCount < 1e10) {
    //      Utils.sendMail("topic:%s, on time:%s, batch-size:%d".format(topics, date, allCount), "srcLog_empty", Seq("zhaoyichen@aiclk.com"))
    //    }


    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaTopicAndPartitionOffset(topicsSet: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    var kafkaCluster = new KafkaCluster(kafkaParams)
    var topicAndPartitions = Set[TopicAndPartition]()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val partitions: Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicsSet)
    partitions match {
      case Left(x) => System.err.println("kafka getPartitions error" + x); System.exit(1)
      case Right(x) =>
        for (partition <- x) {
          topicAndPartitions += partition
        }
    }
    println("***************from kafka TopicAndPartition*******************")
    println(topicAndPartitions)
    val consumerOffset: Either[KafkaCluster.Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
    consumerOffset match {
      case Left(x) => System.err.println("kafka getConsumerOffsets error" + x); System.exit(1)
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

  /**
    * 使用spark streaming从kafka中读取search、show、click等日志，写入hive表中
    *   1. 获取field字段中的日志信息；
    *   2. 调用日志解析函数进行解析
    *
    * @param part DStream中的每个RDD
    * @param key  SrcLog对象中field字段的key，用于获取日志信息
    */
  def getParsedLog(part: rdd.RDD[SrcLog], key: String): rdd.RDD[UnionLog] = {
    //获取log
    val srcDataRdd = part.map {
      x =>
        if (key == "cpc_show_new") {
          x.log_timestamp.toString + x.field.getOrElse(key, null).string_type
        }
        else x.field.getOrElse(key, null).string_type
    }.filter(_ != null)

    //根据不同类型的日志，调用不同的函数进行解析
    var parserLog: rdd.RDD[UnionLog] = null

    if (key == "cpc_search_new") { //search
      parserLog = srcDataRdd.flatMap(x => LogParser.parseSearchLog_v2(x))
      val rdd = parserLog.filter(_ != null)
    } else if (key == "cpc_show_new") { //show
      parserLog = srcDataRdd.map(x => LogParser.parseShowLog(x))
    } else if (key == "cpc_click_new") { //show
      parserLog = srcDataRdd.map {
        x => LogParser.parseClickLog(x)
      }
    } else {
      System.err.println("Can not judge the key of the field.")
      System.exit(1)
    }

    parserLog
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

}