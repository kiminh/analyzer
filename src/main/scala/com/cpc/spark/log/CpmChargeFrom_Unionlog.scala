package com.cpc.spark.log

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.common.LogData
import com.cpc.spark.streaming.tools.{MDBManager, OffsetRedis}
import data.Data
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/*
 覆盖原来的noclick程序，实时统计请求和展示，并且统计cpm计费
 2018-06-06 made by zhj
*/

object CpmChargeFrom_Unionlog {
  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println(
        """
          |Usage : CpmCharge <brokers> <topics> <seconds> <repartition>
          | <brokers> is a list of one or more Kafka brokers
          | <topics> is a list of one or more kafka topics to consume from
          | <out_topics> is the out to kafka topic
          | <repartition> is the thread num that connect to mysql
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, out_topics, sminute, eminute) = args


    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    println(sminute, eminute)

    var sql =
      s"""
         |select raw, day, hour from dl_cpc.cpc_basedata_search_log
         |where day = "2018-12-28" and hour = "18" and minute >= "$sminute" and minute <= "$eminute"
       """.stripMargin
    val search = spark.sql(sql)
    println(sql)

    sql =
      s"""
         |select raw, day, hour from dl_cpc.cpc_basedata_show_log
         |where day = "2018-12-28" and hour = "18" and minute >= "$sminute" and minute <= "$eminute"
         |
       """.stripMargin
    val show = spark.sql(sql)
    println(sql)

    val cpmSearch = search.rdd.repartition(1000)
      .flatMap { x =>
        val raw = x.getString(0)
        val day = x.getString(1)
        val hour = x.getString(2)
        CpmParser.parseReqLog(raw, day, hour.toInt)
      }

    val cpmShow = show.rdd.repartition(1000)
      .flatMap { x =>
        val raw = x.getString(0)
        val day = x.getString(1)
        val hour = x.getString(2)
        CpmParser.parseShowLog(raw, day, hour.toInt)
      }

    println(cpmSearch.count(), cpmShow.count())
    cpmSearch.take(10).foreach(x=>println("search: "+x))
    cpmShow.take(10).foreach(x=>println("show: "+x))

    val messages = cpmSearch.union(cpmShow)

    val parsedData = messages
      .filter(_.isok)
      .map(x => (x.key1, x))
      .groupByKey(1000)
      .map(x => x._2.head)
      //.reduceByKey((x, y) => x) //去重
      .map(x => (x.key2, x))
      .reduceByKey((x, y) => x.sum(y)) //求和
      .map(x => x._2.copy(price = x._2.price / 1000))

    parsedData.take(10).foreach(x => println("parsedData: "+x))


    parsedData.repartition(200)
      .foreachPartition {
          p =>
            val startTime = System.currentTimeMillis()
            var sendSqlExcuteTime: Double = 0
            var sqlExcuteTime: Double = 0

            val conn = MDBManager.getMDBManager(false).getConnection
            val conn2 = MDBManager.getMDB2Manager(false).getConnection
            val dspConn = MDBManager.getDspMDBManager(false).getConnection
            conn.setAutoCommit(false)
            conn2.setAutoCommit(false)
            dspConn.setAutoCommit(false)

            val stmt = conn.createStatement()
            val stmt2 = conn2.createStatement()
            val dspstmt = dspConn.createStatement()
            var foreachCount = 0

            val data_builder = Data.Log.newBuilder()
            val field_builder = Data.Log.Field.newBuilder()
            val map_builder = Data.Log.Field.Map.newBuilder()
            val valueType_builder = Data.ValueType.newBuilder()

            val ts = System.currentTimeMillis()
            var messages = Seq[KeyedMessage[String, Array[Byte]]]()

            /*测试
            p.foreach {
              rec =>
                foreachCount += 1
                val sql = if (rec.adsrc == 1) rec.mkSql else rec.mkSqlDsp
                println(sql)
            }
            println(getCurrentDate("") + ";foreach-count:" + foreachCount)*/

            p.foreach {
              rec =>
                var sql = ""
                foreachCount += 1
                var reset: ResultSet = null
                val sendSqlStartTime = System.currentTimeMillis()
                if (rec.adsrc == 1) {
                  sql = rec.mkSql
                  if (rec.planId >= 1000000 || rec.planId == 0)
                    reset = stmt2.executeQuery(sql)
                  else
                    reset = stmt.executeQuery(sql)
                }
                else {
                  sql = rec.mkSqlDsp
                  println(sql)
                  reset = dspstmt.executeQuery(sql)
                }

                var store_sql_status = 2
                var consumeCoupon = 0
                var consumeBalance = 0
                var executeTime: Double = 0

                while (reset.next()) {
                  try {
                    store_sql_status = reset.getInt(1)
                    consumeBalance = reset.getInt(2)
                    consumeCoupon = reset.getInt(3)
                    executeTime = reset.getDouble(4)
                    sqlExcuteTime = sqlExcuteTime + executeTime
                  } catch {
                    case ex: Exception =>
                      ex.printStackTrace()
                      store_sql_status = 3
                  }
                }
                if (store_sql_status != 0) {
                  println("update sql error:" + sql)
                }
                sendSqlExcuteTime += System.currentTimeMillis() - sendSqlStartTime

                if (rec.price > 0) {
                  //判断是否输出到kafka
                  var arr = Seq[(String, String, String)]()
                  arr = arr :+ ("int", "sql_status", store_sql_status.toString)
                  arr = arr :+ ("int", "balance", consumeBalance.toString)
                  arr = arr :+ ("int", "coupon", consumeCoupon.toString)
                  arr = arr :+ ("string", "sql", sql)
                  arr = arr :+ ("float", "executeTime", executeTime.toString)

                  messages = messages :+ packMessage(data_builder, field_builder, map_builder, valueType_builder,
                    arr, ts, out_topics)
                }
            }

            var kafakaSendTime: Double = 0
            var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
            //for (message <- messages) {
            try {
              val startKafkaTime = System.currentTimeMillis()
              producer.send(messages:_*)
              kafakaSendTime += System.currentTimeMillis() - startKafkaTime
            } catch {
              case ex: Exception =>
                ex.printStackTrace()
                if (producer != null) {
                  producer.close()
                  producer = null
                }
                producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
            }
            //}

            conn.commit()
            conn.close()
            conn2.commit()
            conn2.close()
            dspConn.commit()
            dspConn.close()
            producer.close()
            println(getCurrentDate("") + ";timestart")
            println(getCurrentDate("") + ";totalexcute-milliseconds:" + (System.currentTimeMillis() - startTime))
            println(getCurrentDate("") + ";kafkaexcute-milliseconds:" + kafakaSendTime)
            println(getCurrentDate("") + ";call-excute-milliseconds:" + sqlExcuteTime * 1000)
            println(getCurrentDate("") + ";sendsql-milliseconds:" + sendSqlExcuteTime)
            println(getCurrentDate("") + ";foreach-count:" + foreachCount)
            println(getCurrentDate("") + ";timeend")


        }
  }

  def getKafkaTopicAndPartitionOffset(topicsSet: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
    val kafkaCluster = new KafkaCluster(kafkaParams)
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

  def packMessage(dataBuilder: Data.Log.Builder, fieldBuilder: Data.Log.Field.Builder,
                  mapBuilder: Data.Log.Field.Map.Builder, valueBuilder: Data.ValueType.Builder,
                  arr: Seq[(String, String, String)], ts: Long, out_topics: String): KeyedMessage[String, Array[Byte]] = {

    dataBuilder.clear()
    fieldBuilder.clear()
    mapBuilder.clear()
    valueBuilder.clear()

    for (map <- arr) {
      //(type,key,value)
      map._1 match {
        case "int" => valueBuilder.setIntType(map._3.toInt)
        case "string" => valueBuilder.setStringType(map._3)
        case "float" => valueBuilder.setFloatType(map._3.toFloat)
        case "long" => valueBuilder.setLongType(map._3.toLong)
        case _ =>
      }
      mapBuilder.setKey(map._2)
      mapBuilder.setValue(valueBuilder.build())
      fieldBuilder.addMap(mapBuilder.build())
      mapBuilder.clear()
      valueBuilder.clear()
    }
    dataBuilder.setLogTimestamp(ts)
    dataBuilder.setField(fieldBuilder.build())
    //new KeyedMessage[String, Array[Byte]](out_topics, null, dataBuilder.build().toByteArray)
    new KeyedMessage[String, Array[Byte]](out_topics, Random.nextInt(20.toInt).toString, dataBuilder.build().toByteArray)
  }

  /*def parseSearchShow(logdata: LogData): Seq[CpmLog] = {

    val ts = if (logdata.log.hasLogTimestamp()) logdata.log.getLogTimestamp else 0L

    val (date, hour) = Utils.get_date_hour_from_time(ts)

    val map: data.Data.Log.Field.Map =
      if (logdata.log.hasField() && logdata.log.getField.getMapCount > 0)
        logdata.log.getField.getMap(0)
      else {
        null
      }


    if (ts == 0 || map == null) {
      None
    } else {
      val key = map.getKey
      if (key.startsWith("cpc_search")) {
        parseReqLog(map, date, hour)
      } else if (key.startsWith("cpc_show")) {
        parseShowLog(map, date, hour)
      }
      else None
    }
  }*/
}
