package com.cpc.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.cpc.spark.common.{Event, LogData}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import mlmodel.mlmodel.ProtoPortrait
import com.typesafe.config.ConfigFactory

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

    val spark = SparkSession.builder()
      .appName("ml snapshot from show log")
      .enableHiveSupport()
      .getOrCreate()

    //val sparkConf = new SparkConf().setAppName("ml snapshot: topics = " + topics)
    val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    val base_data = KafkaUtils
      .createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
      .map{
        case (key, v) =>
          try {

            val logdata = LogData.parseData(v)
            val log_timestamp = logdata.log.getLogTimestamp
            val field = logdata.log.getField.getMap(0)
            val rawlog = log_timestamp.toString + field.getValue.getStringType
            val log = Event.parse_show_log(rawlog)
            val event = log.event

            val date = new SimpleDateFormat("yyyy-MM-dd").format(log.timestamp)
            val hour = new SimpleDateFormat("HH").format(log.timestamp)

            val ad = event.getAd
            val media = event.getMedia

            RawFeature(
              searchid = event.getSearchId,
              ideaid = ad.getUnitId,
              unitid = ad.getGroupId,
              planid = ad.getPlanId,
              userid = ad.getUnitId,
              adclass = ad.getClass_,
              adtype = ad.getType.getNumber,
              interact = ad.getInteraction.getNumber,
              slottype = media.getAdslotType.getNumber,
              slotid = media.getAdslotId.toInt,
              uid = event.getDevice.getUid,
              date = date,
              hour = hour
            )
          } catch {
            case e: Exception =>
              null
          }
      }
      .filter(_ != null)

    val conf = ConfigFactory.load()
    base_data.foreachRDD {rdd =>

      val snap = rdd.mapPartitions{p =>
        val redis = new RedisClient(conf.getString("redis.ml_feature_ali.host"),
          conf.getInt("redis.ml_feature_ali.port"))
        redis.auth(conf.getString("redis.ml_feature_ali.auth"))
        val portraits = mutable.Map[String, ProtoPortrait]()
        p.map{x =>
          val vec = mutable.Map[Int, Float]()
          var key = "user%d".format(x.adtype)
          val up = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(up, vec)

          key = "i%d".format(x.ideaid)
          var p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)
          parseUserPortrait(up, "uid#ideaid%s".format(x.ideaid), x.ideaid, vec)

          key = "p%d".format(x.planid)
          p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)
          parseUserPortrait(up, "uid#planid%s".format(x.planid), x.planid, vec)

          key = "un%d".format(x.unitid)
          p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)
          parseUserPortrait(up, "uid#unitid%s".format(x.unitid), x.unitid, vec)

          key = "ac%d".format(x.adclass)
          p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)
          parseUserPortrait(up, "uid#adclass%s".format(x.adclass), x.adclass, vec)

          key = "it%d".format(x.interact)
          p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)
          parseUserPortrait(up, "uid#interact%s".format(x.interact), x.interact, vec)

          key = "at%d".format(x.adtype)
          p = getPortraitFromRedis(key, redis, portraits)
          parsePortrait(p, vec)

          parseUserPortrait(up, "uid#userid%s".format(x.userid), x.userid, vec)
          parseUserPortrait(up, "uid#slottype%s".format(x.slottype), x.slottype, vec)

          val rawInt = Map(
            "ideaid" -> x.ideaid,
            "planid" -> x.planid,
            "unitid" -> x.unitid,
            "userid" -> x.unitid,
            "adclass" -> x.adclass,
            "adtype" -> x.adtype,
            "interact" -> x.interact,
            "slotid" -> x.slotid
          )

          val rawString = Map(
            "uid" -> x.uid
          )

          Snapshot(
            searchid = x.searchid,
            raw_int = rawInt,
            raw_string = rawString,
            feature_vector = vec,
            date = x.date,
            hour = x.hour
          )
        }
      }

      val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
      val keys = snap.map{ x => (x.date, x.hour) }.distinct.toLocalIterator
      keys.foreach {key =>
        val part = snap.filter(r => r.date == key._1 && r.hour == key._2)
        val numbs = part.count()

        val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
        println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))
        println(part.first())

        if (numbs > 0) {
          val table = "ml_snapshot_test"
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

  def parseUserPortrait(up: ProtoPortrait, key: String, id: Int, vec: mutable.Map[Int, Float]): Unit = {
    if (up.subMap.isDefinedAt(key)) {
      val sub = up.subMap.get(key).get
      parsePortrait(sub, vec)
    }
  }

  def parsePortrait(p: ProtoPortrait, vec: mutable.Map[Int, Float]): Unit = {
    if (p != null && p.valueMap != null && p.valueMap.size > 0) {
      for ((k, v) <- p.valueMap) {
        vec.update(k.toInt, v)
      }
    }
  }

  def getPortraitFromRedis(key: String, redis: RedisClient,
                           portraits: mutable.Map[String, ProtoPortrait]): ProtoPortrait = {
    if (!portraits.isDefinedAt(key)) {
      val d = redis.get[Array[Byte]](key)
      if (d.isDefined) {
        val p = ProtoPortrait.parseFrom(d.get)
        portraits.update(key, p)
      } else {
        portraits.update(key, null)
      }
    }
    portraits(key)
  }

  case class RawFeature(
                       searchid: String = "",
                       ideaid: Int = 0,
                       unitid: Int = 0,
                       planid: Int = 0,
                       userid: Int = 0,
                       adclass: Int = 0,
                       adtype: Int = 0,
                       interact: Int = 0,
                       slottype: Int = 0,
                       slotid: Int = 0,
                       uid: String = "",
                       date: String = "",
                       hour: String = ""
                       )

  case class Snapshot(
                     searchid: String = "",
                     version: String = "",
                     model_name: String = "",
                     data_type: Int = 0,
                     strategy: String = "",
                     result: Float = 0,
                     calibrated: Float = 0,
                     raw_int: collection.Map[String, Int] = null,
                     raw_string: collection.Map[String, String] = null,
                     feature_vector: collection.Map[Int, Float] = null,
                     date: String = "",
                     hour: String = ""
                     ){

  }

}