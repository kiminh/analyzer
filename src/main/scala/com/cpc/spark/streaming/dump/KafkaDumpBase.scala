package com.cpc.spark.streaming.dump

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.streaming.tools.OffsetRedis
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}

/**
  * author: huazhenhao
  * date: 11/7/18
  */
abstract class KafkaDumpBase[K <: scala.Product] (
                       val inputTopicName: String,
                       val redisOffsetKey: String,
                       val outputDirName: String,
                       val numPerPartition: Int  = 1000000,
                       val brokers: String = "192.168.80.35:9092,192.168.80.36:9092,192.168.80.37:9092,192.168.80.88:9092,192.168.80.89:9092",
                       val batchDurationSeconds: Int = 600) extends java.io.Serializable {

  val offsetRedis = new OffsetRedis()
  offsetRedis.setRedisKey(redisOffsetKey)

  def run(): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("topic:" + inputTopicName)
    val ssc = new StreamingContext(sparkConf, Seconds(batchDurationSeconds))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
    try {
      val partitions = offsetRedis.getPartitionByTopic(inputTopicName)
      for (partition <- partitions) {
        println("topic:" + inputTopicName + ";partition:" + partition)
        val tp = TopicAndPartition(inputTopicName, partition.toInt)
        fromOffsets += (tp -> offsetRedis.getTopicAndPartitionOffSet(inputTopicName, partition.toInt))
      }
    } catch {
      case t: Exception => System.err.println("connect redis exception" + t.getStackTrace)
        fromOffsets = Map()
    }
    println("***************redis fromOffsets*******************")
    println(fromOffsets)

    var kafkaOffset: Map[TopicAndPartition, Long] = null
    if (fromOffsets.nonEmpty) {
      kafkaOffset = getKafkaTopicAndPartitionOffset(inputTopicName, kafkaParams)
      println("***************from kafka offsets*******************")
      println(kafkaOffset)
      kafkaOffset.foreach(tp => {
        try {
          //可能key不存在
          if (fromOffsets.contains(tp._1)) {
            if (fromOffsets(tp._1) < tp._2) {
              kafkaOffset += (tp._1 -> fromOffsets(tp._1))

            }
          }

        } catch {
          case t: Throwable =>
            t.printStackTrace()
            println("set kafkaOffset key filled" + t.printStackTrace()) // TODO: handle error
        }
      })
    }
    println("***************last offsets*******************")
    println(kafkaOffset)

    var messages: InputDStream[(String, Array[Byte])] = null

    if (kafkaOffset == null || kafkaOffset.isEmpty) {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, Set(inputTopicName))
      println("no offset")
    } else {
      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaOffset, messageHandler)
      println("from offset")
    }

    messages.foreachRDD {
      rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreachPartition { _ =>
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
    printCurrentTime("end-offsetRanges")

    val base_data = messages.map {
      case (_, v) =>
        try {
          parseFunc(v)
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
        val keys = rs.map { x => getTimeKeyFunc(x.asInstanceOf[K]) }.distinct().toLocalIterator
        val spark = SparkSession.builder().config(ssc.sparkContext.getConf).enableHiveSupport().getOrCreate()

        keys.foreach {
          key =>
            val part = rs.filter(r => {
              val rTime = getTimeKeyFunc(r.asInstanceOf[K])
              rTime._1 == key._1 && rTime._2 == key._2 && rTime._3 == key._3
            })
            val numbs = part.count()
            val outFilesNum = (numbs / numPerPartition + 1).toInt //用于动态减少输出文件数

            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
            println("~~~~~~~~~ %s ~~~~~~ on time:%s  batch-size:%d".format(inputTopicName, date, numbs))

            if (numbs > 0) {
              getDF(part.asInstanceOf[RDD[K]], spark)
//              spark.createDataFrame[K](part.asInstanceOf[RDD[K]])
                .repartition(outFilesNum)
                .write
                .mode(SaveMode.Append)
                .parquet("/warehouse/dl_cpc.db/%s/%s/%s/%s".format(outputDirName, key._1, key._2, key._3))

              val sqlStmt =
                """
                  |ALTER TABLE dl_cpc.%s add if not exists PARTITION (thedate = "%s", thehour = "%s", theminute = "%s")  LOCATION
                  |       '/warehouse/dl_cpc.db/%s/%s/%s/%s'
                  |
                    """.stripMargin.format(outputDirName, key._1, key._2, key._3, outputDirName, key._1, key._2, key._3)
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

  def getDF(rdd: RDD[K], spark: SparkSession): DataFrame

  def parseFunc(bytes: Array[Byte]) : K

  def getTimeKeyFunc(element: K): (String, String, String)

  def getKafkaTopicAndPartitionOffset(topic: String, kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    val kafkaCluster = new KafkaCluster(kafkaParams)
    var topicAndPartitions = Set[TopicAndPartition]()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val partitions: Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
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

  def printCurrentTime(message: String) {
    val dtStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
    if (message.length() > 0) {
      println("currentDate:" + message + ":" + dtStr)
    } else {
      print("currentDate:" + dtStr)
    }
  }
}
