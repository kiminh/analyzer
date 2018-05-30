package com.cpc.spark.streaming.anal

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import mlmodel.mlmodel.AccessLog
import org.apache.spark.streaming.kafka._

import scala.collection.mutable

object MLSnapshot {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
            haha
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val Array(brokers, topics, seconds) = args
    println(args.mkString(" "))

    val sparkConf = new SparkConf().setAppName("ml snapshot: topics = " + topics)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val base_data = KafkaUtils
      .createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      .flatMap {
        case (k, v) =>
          try {
            val alog = AccessLog.parseFrom(v)
            val date = new SimpleDateFormat("yyyy-MM-dd").format(alog.timestamp)
            val hour = new SimpleDateFormat("HH").format(alog.timestamp)

            alog.snapshots
              .map {
                v =>
                  Snapshot(
                    searchid = alog.searchid,
                    version = v.reqVersion,
                    model_name = v.modelName,
                    data_type = v.dataType,
                    strategy = v.strategy.name,
                    result = v.result,
                    calibrated = v.calibrated,
                    feature_vector = v.featureVector,
                    date = date,
                    hour = hour
                  )
              }
          } catch {
            case t: Throwable =>
              t.printStackTrace() // TODO: handle error
              Seq()
          }
      }

    base_data.print()
    val spark = SparkSession.builder()
      .appName("ml snapshot")
      .enableHiveSupport()
      .getOrCreate()

    var date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
    base_data.foreachRDD {
      rs =>
        val keys = rs.map{ x => (x.date, x.hour) }.distinct.toLocalIterator
        keys.foreach { //(日期，小时)
          key =>
            val part = rs.filter(r => r.date == key._1 && r.hour == key._2)
            val numbs = part.count()
            
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
            println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))
            println(part.first())

            if (numbs > 0) {
              val table = "ml_snapshot"
              spark.createDataFrame(part)
                .write
                .mode(SaveMode.Append)
                .parquet("/warehouse/dl_cpc.db/%s/%s/%s".format(table, key._1, key._2))

              val sqlStmt =
                """
                  |ALTER TABLE dl_cpc.%s add if not exists PARTITION (`date` = "%s", hour = "%s")
                  | LOCATION '/warehouse/dl_cpc.db/%s/%s/%s'
                  |
                """.stripMargin.format(table, key._1, key._2, table, key._1, key._2)
              println(sqlStmt)
              spark.sql(sqlStmt)
            }
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  case class Snapshot(
                     searchid: String = "",
                     version: String = "",
                     model_name: String = "",
                     data_type: Int = 0,
                     strategy: String = "",
                     result: Float = 0,
                     calibrated: Float = 0,
                     feature_vector: collection.Map[Int, Float] = null,
                     date: String = "",
                     hour: String = ""
                     ){

  }

}