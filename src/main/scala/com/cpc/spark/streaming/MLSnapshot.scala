package com.cpc.spark.streaming

import java.text.SimpleDateFormat
import java.util.Date

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import com.cpc.spark.common.{Event, LogData}
import com.cpc.spark.streaming.tools.Data2Kafka
import redis.clients.jedis.JedisPool
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import mlmodel.mlmodel.ProtoPortrait
import com.typesafe.config.ConfigFactory
import org.apache.spark.HashPartitioner
import scala.collection.{mutable => cmutable}
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
        val Array(brokers, topics, seconds, oldbrokers) = args
        println(args.mkString(" "))

        val spark = SparkSession.builder()
          .appName("ml snapshot from show log")
          .enableHiveSupport()
          .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .getOrCreate()

        //val sparkConf = new SparkConf().setAppName("ml snapshot: topics = " + topics)
        val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

        //MLSnapshot报警日志发送的kafka topic
        val cpc_mlsnapshot_warning = "cpc_realtime_parsedlog_warning"

        //初始化DStream 每个batch的开始时间； 用于报警服务
        var currentBatchStartTime = 0L

        val messages = KafkaUtils
          .createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)

        messages.foreachRDD { rdd =>
            //每个batch的开始时间
            currentBatchStartTime = new Date().getTime
        }

        val base_data = messages.map {
            case (key, v) =>
                try {
                    val logdata = LogData.parseData(v)
                    val log_timestamp = logdata.log.getLogTimestamp
                    val field = logdata.log.getField.getMap(0)
                    val rawlog = log_timestamp.toString + field.getValue.getStringType
                    val log = Event.parse_show_log(rawlog)
                    val event = log.event

                    val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
                    val hour = new SimpleDateFormat("HH").format(log_timestamp)
                    val ad = event.getAd
                    val media = event.getMedia

                    var uid = event.getDevice.getUid
                    //设备号过滤掉 ip
                    if (uid.contains(".") || uid.contains("000000")) {
                        uid = ""
                    }

                    RawFeature(
                        searchid = event.getSearchId,
                        timestamp = log_timestamp,
                        ideaid = ad.getUnitId,
                        unitid = ad.getGroupId,
                        planid = ad.getPlanId,
                        userid = ad.getUserId,
                        adclass = ad.getClass_,
                        adtype = ad.getType.getNumber,
                        interact = ad.getInteraction.getNumber,
                        slottype = media.getAdslotType.getNumber,
                        slotid = media.getAdslotId.toInt,
                        uid = uid,
                        date = date,
                        hour = hour
                    )
                } catch {
                    case e: Exception =>
                        null
                }
        }.filter(x => x != null && x.searchid.length > 0 && x.ideaid > 0)

        val conf = ConfigFactory.load()

        base_data.foreachRDD { rdd =>
            val r = rdd.mapPartitions(f => {
                val result = scala.collection.mutable.ListBuffer[(String,RawFeature)]()
                val list = f.toIterator
                list.foreach(x => {
                    val tmp = (x.uid,x)
                    result += tmp
                })
                result.toIterator
            }).partitionBy(new HashPartitioner(500)).cache().mapPartitions(f => {
                val result = scala.collection.mutable.ListBuffer[RawFeature]()
                val list = f.toIterator
                list.foreach(x => {
                    val tmp = x._2
                    result += tmp
                })
                result.toIterator
            })
            //val r = rdd.map(f => (f.uid, f)).partitionBy(new HashPartitioner(100)).map(f => f._2).cache()

            val snap = r.mapPartitions { p =>
                val redis = new RedisClient(conf.getString("redis.ml_feature_ali.host"),
                    conf.getInt("redis.ml_feature_ali.port"))
                redis.auth(conf.getString("redis.ml_feature_ali.auth"))
                val portraits = cmutable.HashMap[String, ProtoPortrait]()


                p.map { x =>
                    val vec = cmutable.Map[Int, Float]()

                    var key = "u%s".format(x.uid)
                    val up = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(up, vec)

                    key = "i%d".format(x.ideaid)
                    var p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#ideaid", x.ideaid, vec)

                    key = "p%d".format(x.planid)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#planid", x.planid, vec)

                    key = "un%d".format(x.unitid)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#unitid", x.unitid, vec)

                    key = "ac%d".format(x.adclass)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#adclass", x.adclass, vec)

                    key = "it%d".format(x.interact)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#interact", x.interact, vec)

                    key = "at%d".format(x.adtype)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#adtype", x.adtype, vec)

                    key = "user%d".format(x.userid)
                    p = getPortraitFromRedis(key, redis, portraits)
                    parsePortrait(p, vec)
                    parseUserPortrait(up, "uid#userid", x.ideaid, vec)

                    parseUserPortrait(up, "uid#slottype", x.slottype, vec)

                    val rawInt = Map(
                        "timestamp" -> (x.timestamp / 1000).toInt,
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
            }.cache()

            val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
            val keys = snap.mapPartitions(f => {
                val result = scala.collection.mutable.ListBuffer[((String,String),Int)]()
                val list = f.toIterator
                list.foreach(f => {
                    val tmp = ((f.date,f.hour),1)
                    result += tmp
                })
                result.toIterator
            }).reduceByKey((x,y) => x).mapPartitions(f => {
                val result = scala.collection.mutable.ListBuffer[(String,String)]()
                val list = f.toIterator
                list.foreach(f => {
                    val tmp = f._1
                    result += tmp
                })
                result.toIterator
            }).collect
            //val keys = snap.map(f => ((f.date,f.hour),1)).reduceByKey((x,y) => x).map(f => f._1).toLocalIterator
            System.out.println("******keys 's num is " + keys.length + " ******")
            //val keys = snap.map { x => (x.date, x.hour) }.distinct.toLocalIterator
            keys.foreach { key =>
                val part = snap.filter(r => r.date == key._1 && r.hour == key._2)
                //val numbs = part.count()

                //val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date().getTime)
                //println("~~~~~~~~~ zyc_log ~~~~~~ on time:%s  batch-size:%d".format(date, numbs))
                //println(part.first())

                //if (numbs > 0) {
                val table = "ml_snapshot_from_show"
                spark.createDataFrame(part)
                  .coalesce(100)
                  .write
                  .mode(SaveMode.Append)
                  .parquet("/warehouse/dl_cpc.db/%s/%s/%s".format(table, key._1, key._2))

                val sqlStmt =
                    """
                      |ALTER TABLE dl_cpc.%s add if not exists PARTITION (`date` = "%s", hour = "%s")
                      | LOCATION '/warehouse/dl_cpc.db/%s/%s/%s'
                    """.stripMargin.format(table, key._1, key._2, table, key._1, key._2)
                println(sqlStmt)
                spark.sql(sqlStmt)
                //}
            }
            /**
              * 报警日志写入kafka的topic: cpc_realtime_parsedlog_warning
              */
            // 每个batch的结束时间
            val currentBatchEndTime = new Date().getTime
            val costTime = (currentBatchEndTime - currentBatchStartTime) / 1000.0

            val data2Kafka = new Data2Kafka()
            val mapString: Seq[(String, String)] = Seq(("Topic", "mlSnapshot_cpc_show_new"))
            val mapFloat: Seq[(String, Float)] = Seq(("ProcessingTime", costTime.toFloat))
            data2Kafka.setMessage(currentBatchEndTime, null, mapFloat, null, mapString)
            data2Kafka.sendMessage(oldbrokers, cpc_mlsnapshot_warning)
            data2Kafka.close()
        }

        ssc.start()
        ssc.awaitTermination()
    }

    def parseUserPortrait(up: ProtoPortrait, key: String, id: Int, vec: cmutable.Map[Int, Float]): Unit = {
        if (up != null) {
            if (up.subMap.isDefinedAt(key)) {
                val sub = up.subMap.get(key).get
                val idKey = s"$id"
                if (sub.subMap.isDefinedAt(idKey)) {
                    val sub2 = sub.subMap.get(idKey).get
                    parsePortrait(sub2, vec)
                }
            }
        }
    }

    def parsePortrait(p: ProtoPortrait, vec: cmutable.Map[Int, Float]): Unit = {
        if (p != null && p.valueMap != null && p.valueMap.size > 0) {
            for ((k, v) <- p.valueMap) {
                vec.update(k.toInt, v)
            }
        }
    }

    def getPortraitFromRedis(key: String, redis: RedisClient,
                             portraits: cmutable.HashMap[String, ProtoPortrait]): ProtoPortrait = {
        if (portraits.size > 700 * 10000) {
            null
        } else {
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
    }

    case class RawFeature(
                           searchid: String = "",
                           timestamp: Long = 0,
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
                       ) {

    }

}