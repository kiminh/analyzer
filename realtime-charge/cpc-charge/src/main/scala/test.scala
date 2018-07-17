import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.LogData
import com.cpc.spark.streaming.anal.CpcStreamingAnalNoClickV2.{getCurrentDate, getKafkaTopicAndPartitionOffset}
import com.cpc.spark.streaming.parser.StreamingDataParserV2
import com.cpc.spark.streaming.tools.OffsetRedis
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test {
  def main1(args: Array[String]): Unit = {
    if (args.length < 6) {
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
    val Array(brokers, out_topics, seconds, repartition, env, isPrintFailed) = args
    val topics = "cpc_search,cpc_show,cpc_search_new,cpc_show_new"
    val sparkConf = new SparkConf().setAppName("cpc-charge-test " + topics + " env :" + env)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
    try {
      for (topic <- topicsSet) {
        var partitions = OffsetRedis.getOffsetRedis.getPartitionByTopic(topic)
        for (partition <- partitions) {
          println("topic:" + topic + ";partition:" + partition)
          val tp = TopicAndPartition(topic, partition.toInt)
          fromOffsets += (tp -> OffsetRedis.getOffsetRedis.getTopicAndPartitionOffSet(topic, partition.toInt))
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
          if (fromOffsets.nonEmpty && fromOffsets.contains(tp._1)) {
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


    val parserData = messages.map {
      case (k, v) =>
        try {
          val logdata = LogData.parseData(v)
          StreamingDataParserV2.streaming_data_parse(logdata)
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
            (false, 0, "", "", "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        }
    }

    if (isPrintFailed.length() > 0) {
      parserData.filter(!_._1).count().map {
        cnt =>
          var now: Date = new Date()
          var dateFormat: SimpleDateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss")
          var hehe = dateFormat.format(now)
          hehe + ":parserFailedCount:" + cnt
      }.print()
    }
    val base_data = parserData.filter {
      case (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        isok
    }.map {
      case (isok, adSrc, dspMediaId, dspAdslotId, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        ((sid, typed), (adSrc, dspMediaId, dspAdslotId, date, hour, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) => x
    }.map {
      case ((sid, typed), (adSrc, dspMediaId, dspAdslotId, date, hour, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click))
      => ((adSrc, dspMediaId, dspAdslotId, date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
    }.map {
      case ((adSrc, dspMediaId, dspAdslotId, date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click)) =>
        (media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price, adSrc, dspMediaId, dspAdslotId)
    }
    base_data.foreachRDD(rdd => {
      val start_time = System.currentTimeMillis()
      rdd.count()
      println("**********************************************")
      println("total time [%d]".format((System.currentTimeMillis() - start_time) / 1000))
      println("**********************************************")
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    val a = Seq[String]("a")
    println("length:" + a.length)
    println("size:" + a.size)
  }
}
