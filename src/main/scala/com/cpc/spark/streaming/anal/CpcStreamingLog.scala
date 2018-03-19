package com.cpc.spark.streaming.anal

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import kafka.common.TopicAndPartition
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.cpc.spark.common.{LogData, Utils}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


object CpcStreamingLog {
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

    var messages: InputDStream[(String, Array[Byte])] =
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)

    val base_data = messages.map {
      case (k, v) =>
        try {
          val logdata = LogData.parseData(v)
          val log_timestamp = logdata.log.getLogTimestamp
          val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
          val hour = new SimpleDateFormat("HH").format(log_timestamp)

          val field2 = scala.collection.mutable.Map[String, ExtValue]()
          val fieldCount = logdata.log.getField.getMapCount
          for (i <- 0 to fieldCount - 1) {
            val field = logdata.log.getField.getMap(i)
            val extValue = ExtValue(field.getValue.getIntType, field.getValue.getLongType, field.getValue.getFloatType, field.getValue.getStringType)
            field2 += (field.getKey -> extValue)

          }

          val field: collection.Map[String, ExtValue] = field2.toMap
          SrcLog(logdata.log.getLogTimestamp, logdata.log.getIp, field, date, hour)
        } catch {
          case t: Throwable =>
            t.printStackTrace() // TODO: handle error
            null
        }
    }
      .filter(_ != null)
    base_data.print()

    val spark = SparkSession.builder()
      .appName("zyc_kafka_test")
      .enableHiveSupport()
      .getOrCreate()

    var allCount: Long = 0
    var date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
    base_data.foreachRDD {
      rs =>
        val keys = rs.map {
          x =>
            (x.thedate, x.thehour)
        }
          .distinct().toLocalIterator


        keys.foreach { //(日期，小时)
          key =>
            val part = rs.filter(r => r.thedate == key._1 && r.thehour == key._2)
            val numbs = part.count()
            allCount += numbs
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
            println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))

            if (numbs > 0) {
              spark.createDataFrame(part)
                .toDF("log_timestamp", "ip", "field", "thedate", "thehour")
                .write
                .mode(SaveMode.Append)
                .parquet("/warehouse/dl_cpc.db/src_%s/%s/%s".format(topics, key._1, key._2))

              val isExistsSql =
                """
                  |SELECT 1 from dl_cpc.src_%s WHERE thedate = "%s" and thehour = "%s" limit 1
                  |
                """.stripMargin.format(topics, key._1, key._2)
              println(isExistsSql)
              val isExists = spark.sql(isExistsSql)

              if (isExists.count() == 0) {
                val sqlStmt =
                  """
                    |ALTER TABLE dl_cpc.src_%s add PARTITION (thedate = "%s", thehour = "%s")  LOCATION
                    |       '/warehouse/dl_cpc.db/src_%s/%s/%s'
                    |
                """.stripMargin.format(topics, key._1, key._2, topics, key._1, key._2)
                println(sqlStmt)
                spark.sql(sqlStmt)
              }

            }
        }
    }
//    if (allCount < 1e10) {
//      Utils.sendMail("topic:%s, on time:%s, batch-size:%d".format(topics, date, allCount), "srcLog_empty", Seq("zhaoyichen@aiclk.com"))
//    }


    ssc.start()
    ssc.awaitTermination()
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


  case class SrcLog(
                     log_timestamp: Long = 0,
                     ip: String = "",
                     field: collection.Map[String, ExtValue] = null,
                     thedate: String = "",
                     thehour: String = ""
                   )

  case class ExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")

}