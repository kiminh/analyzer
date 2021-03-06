package com.cpc.spark.streaming.anal

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.LogData
import com.cpc.spark.log.parser._
import com.cpc.spark.streaming.tools.{Data2Kafka, OffsetRedis}
import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, TaskContext}


object CpcStreamingShowLogParser {

  /**
    * 创建一个offsetRedis对象
    * 调用方法设置Redis的key
    */
  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey("PARSED_LOG_KAFKA_OFFSET")

  val data2Kafka = new Data2Kafka()

  val cpc_realtime_parsedlog_warning = "cpc_realtime_parsedlog_warning"
  var currentBatchStartTime=0L

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

    var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)

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
        currentBatchStartTime = new Date().getTime

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
      .filter(_ != null).repartition(1000)

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
              //修改配置
              val table = conf.getString("topic2tbl_logparsed." + topics.split(",")(0))

              val topicKey = topics.split(",")(0)

              //获取log
              val srcDataRdd = part.map {
                x => x.log_timestamp.toString + x.field.getOrElse(topicKey, null).string_type
              }.filter(_ != null)

              //调用函数进行解析
              val parsedLog = srcDataRdd.map(x => LogParser.parseClickLog_v2(x))
                .filter(_ != null)
                .coalesce(200, false)

              spark.createDataFrame(parsedLog)
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

        /**
          * 报警日志写入kafka的topic: cpc_realtime_parsedlog_warning
          */
        // 每个batch的结束时间
        val currentBatchEndTime = new Date().getTime
        val costTime = (currentBatchEndTime - currentBatchStartTime) / 1000.0

        val mapString: Seq[(String, String)] = Seq(("Topic", topics.split(",")(0)))
        val mapFloat: Seq[(String, Float)] = Seq(("ProcessingTime", costTime.toFloat))
        data2Kafka.clear()
        data2Kafka.setMessage(currentBatchEndTime, null, mapFloat, null, mapString)
        data2Kafka.sendMessage(brokers,cpc_realtime_parsedlog_warning)
        data2Kafka.close()


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




  def getParsedShowLog(part: RDD[SrcLog], topic: String, spark: SparkSession, table: String, key: (String, String, String)): Unit = {

    //获取log
    val srcDataRdd = part.map {
      x => x.log_timestamp.toString + x.field.getOrElse(topic, null).string_type
    }.filter(_ != null)

    /*
      根据不同类型的日志，调用不同的函数进行解析

      Null value appeared in non-nullable field
      java.lang.NullPointerException: Null value appeared in non-nullable field: top level row object
      If the schema is inferred from a Scala tuple/case class, or a Java bean, please try to use
      scala.Option[_] or other nullable types (e.g. java.lang.Integer instead of int/scala.Int).

      解决方法： 过滤null值
     */
    val parsedLog = srcDataRdd.map { x => LogParser.parseShowLog_v2(x) }.filter(_ != null).coalesce(200, true)

    spark.createDataFrame(parsedLog)
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