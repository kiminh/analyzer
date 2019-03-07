//package com.cpc.spark.OcpcProtoType.streaming
//
//import java.text.SimpleDateFormat
//import java.util.Calendar
//
//import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql3
//import com.cpc.spark.ocpcV3.utils
//import com.typesafe.config.ConfigFactory
//import org.apache.spark.sql._
//import org.apache.spark.sql.functions._
//
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
//import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import com.cpc.spark.streaming.tools.{Data2Kafka, OffsetRedis}
//import kafka.common.TopicAndPartition
//import kafka.message.MessageAndMetadata
//import kafka.serializer.{DefaultDecoder, StringDecoder}
//
//object OcpcConsumeMonitor {
//  val offsetRedis = new OffsetRedis()
//  offsetRedis.setRedisKey("PARSED_LOG_KAFKA_OFFSET")
//  val data2Kafka = new Data2Kafka()
//
//  var currentBatchStartTime=0L
//
//  def main(args: Array[String]): Unit = {
//    val Array(brokers, topics, out_topics, seconds) = args
//
//    val spark = SparkSession.builder()
//      .appName("ocpc unitid consumes: topics = " + topics)
//      .enableHiveSupport()
//      .getOrCreate()
//
//    import spark.implicits._
//
//    val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds.toInt))
//
//    val topicsSet = topics.split(",").toSet
//
//    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
//
//    var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
//
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
//    try {
//      for (topic <- topicsSet) {
//        val partitions = offsetRedis.getPartitionByTopic(topic)
//        for (partition <- partitions) {
//          //println("topic:" + topic + ";partition:" + partition)
//          val tp = TopicAndPartition(topic, partition.toInt)
//          fromOffsets += (tp -> offsetRedis.getTopicAndPartitionOffSet(topic, partition.toInt))
//        }
//      }
//    } catch {
//      case t: Exception => System.err.println("connect redis exception" + t.getStackTrace)
//        fromOffsets = Map()
//    }
//    //println("***************redis fromOffsets*******************")
//
//    //println(fromOffsets)
//
//    var kafkaoffset: Map[TopicAndPartition, Long] = null
//
//    if (fromOffsets.nonEmpty) {
//      kafkaoffset = getKafkaTopicAndPartitionOffset(topicsSet, kafkaParams)
//      //println("***************from kafka offsets*******************")
//      //println(kafkaoffset)
//      kafkaoffset.foreach(tp => {
//        try {
//          //可能key不存在
//          if (fromOffsets.contains(tp._1)) {
//            if (fromOffsets(tp._1) < tp._2) {
//              kafkaoffset += (tp._1 -> fromOffsets(tp._1))
//
//            }
//          }
//
//        } catch {
//          case t: Throwable =>
//            t.printStackTrace()
//            println("set kafkaoffset key filled" + t.printStackTrace()) // TODO: handle error
//        }
//      })
//    }
//    //println("***************last offsets*******************")
//    //println(kafkaoffset)
//
//    var messages: InputDStream[(String, Array[Byte])] = null
//
//    if (kafkaoffset == null || kafkaoffset.isEmpty) {
//      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
//      println("no offset")
//    } else {
//      messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
//      println("from offset")
//    }
//
//    messages.foreachRDD {
//      rdd => {
//        //每个batch的开始时间
//        currentBatchStartTime = new Date().getTime
//
//        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//        rdd.foreachPartition { iter =>
//          try {
//            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
//            offsetRedis.setTopicAndPartitionOffSet(o.topic, o.partition, o.fromOffset)
//          } catch {
//            case t: Throwable =>
//              t.printStackTrace()
//              println("connect redis exception in offsetRanges " + t.getStackTrace)
//          }
//
//        }
//      }
//    }
//    getCurrentDate("end-offsetRanges")
//    val base_data = messages.map {
//      case (k, v) =>
//        try {
//          val logdata = LogData.parseData(v)
//          val log_timestamp = logdata.log.getLogTimestamp
//          val date = new SimpleDateFormat("yyyy-MM-dd").format(log_timestamp)
//          val hour = new SimpleDateFormat("HH").format(log_timestamp)
//          val minute = new SimpleDateFormat("mm").format(log_timestamp).charAt(0) + "0"
//
//          val field2 = scala.collection.mutable.Map[String, ExtValue]()
//
//          val fieldCount = logdata.log.getField.getMapCount
//          for (i <- 0 until fieldCount) {
//            val field = logdata.log.getField.getMap(i)
//            val extValue = ExtValue(field.getValue.getIntType, field.getValue.getLongType, field.getValue.getFloatType, field.getValue.getStringType)
//            field2 += (field.getKey -> extValue)
//
//          }
//
//          val field: collection.Map[String, ExtValue] = field2.toMap
//          SrcLog(logdata.log.getLogTimestamp, logdata.log.getIp, field, date, hour, minute)
//        } catch {
//          case t: Throwable =>
//            t.printStackTrace() // TODO: handle error
//            null
//        }
//    }
//      .filter(_ != null).repartition(1000)
//
//    //base_data.print()
//
//    base_data.foreachRDD{
//      (rdd,time) =>
//        val t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time.milliseconds))
//        val date = t.substring(0,10)
//        val hour = t.substring(11,13)
//        val minute = t.substring(14,16)
//        println(date,hour,minute)
//
//        val topic = "cpc_show_new"
//
//        val srcDataRDD = rdd.map(x => x.log_timestamp.toString + x.field.getOrElse(topic,null).string_type)
//          .filter(_ != null)
//
//        val log = srcDataRDD.map(x => {
//          val data = Event.parse_show_log(x)
//          if (data != null) {
//            val body = data.event
//
//            val ideaid = body.getAd.getUnitId
//
//            val styleid = body.getAd.getStyleId
//
//            (ideaid,styleid)
//          }
//          else
//            null
//        })
//        if (log != null) {
//          //获取历史数据
//          val coin_black_ideaid = spark.sql(s"select * from dl_cpc.coin_black_ideaid where `date` = '$date' and hour = '$hour'")
//            .rdd
//            .map(x =>
//              (x.getAs[Int]("ideaid"),
//                (x.getAs[Int]("show_num"),x.getAs[Int]("coin_show_num"))
//              )
//            )
//          //历史跟现在得数据进行合并
//          val coin_black = log.filter(x => x != null && x._1 > 0)
//            .map(x => {
//              if (x._2 == 510127)
//                (x._1, (1, 1))
//              else
//                (x._1, (1, 0))
//            })
//            .union(coin_black_ideaid)
//            .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
//            .map(x => CoinBlackIdeaid(ideaid = x._1,
//              show_num = x._2._1,
//              coin_show_num = x._2._2,
//              date = date,
//              hour = hour))
//            .cache()
//
//          //println("coin_black 's count is " + coin_black.count())
//
//          //更新表
//          coin_black.toDF()
//            .repartition(1)
//            .write
//            .mode("overwrite")
//            .insertInto("dl_cpc.coin_black_ideaid")
//          println("insert into dl_cpc.coin_black_ideaid success !")
//          //过滤出金币超过10%的
//          val coin_rate = coin_black.map(x =>
//            CoinBlackIdeaidPb(ideaid = x.ideaid,
//              show_num = x.show_num,
//              coin_show_num = x.coin_show_num,
//              coin_show_rate = x.coin_show_num * 1.0 / x.show_num,
//              date = date,
//              hour = hour,
//              minute = minute)
//          ).filter(_.coin_show_rate > 0.1)
//            .cache()
//
//          coin_rate.toDF()
//            .repartition(1)
//            .write
//            .mode("overwrite")
//            .insertInto("dl_cpc.coin_black_ideaid_pb")
//          println("insert into dl_cpc.coin_black_ideaid_pb success!")
//          //println("coin_rate 's count is " + coin_rate.count())
//
//          coin_rate.unpersist()
//          coin_black.unpersist()
//
//          val coinblacklistBuffer = scala.collection.mutable.ListBuffer[coinideaid]()
//
//          coin_rate.collect().foreach(x => {
//            coinblacklistBuffer += coinideaid(ideaid = x.ideaid)
//            //println(x)
//          })
//
//          val cbl = coinblacklistBuffer.toArray
//
//          val coin = coinblacklist(cilist = cbl)
//
//          coin.writeTo(new FileOutputStream("coin_black_ideaid.pb"))
//
//          val md5 = "sh md5.sh"
//          val md5ret = md5 !
//
//          //val work = "scp coin_black_ideaid.pb work@192.168.80.219:/home/work/mlcpp/data/coin_black_ideaid.pb"
//          //val workret = work !
//          //val workmd5 = "scp coin_black_ideaid.pb.md5 work@192.168.80.219:/home/work/mlcpp/data/coin_black_ideaid.pb.md5"
//          //val workmd5ret = work !
//          val cpc = "scp coin_black_ideaid.pb cpc@192.168.80.23:/home/cpc/model_server/data/coin_black_ideaid.pb"
//          val cpcret = cpc !
//          val cpcmd5 = "scp coin_black_ideaid.pb.md5 cpc@192.168.80.23:/home/cpc/model_server/data/coin_black_ideaid.pb.md5"
//          val cpcmd5ret = cpcmd5 !
//
//          //                    val re = ""
//          //                    println("md5ret is " + md5ret)
//          //                    println("workret is " + workret)
//          //                    println("workmd5ret is " + workmd5ret)
//          //                    println("cpcret is " + cpcret)
//          //                    println("cpcmd5ret is " + cpcmd5ret)
//        }
//
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//  def getKafkaTopicAndPartitionOffset(topicsSet: Set[String], kafkaParams: Map[String, String]): Map[TopicAndPartition, Long] = {
//    var kafkaCluster = new KafkaCluster(kafkaParams)
//    var topicAndPartitions = Set[TopicAndPartition]()
//    var fromOffsets: Map[TopicAndPartition, Long] = Map()
//    val partitions: Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicsSet)
//    partitions match {
//      case Left(x) => System.err.println("kafka getPartitions error" + x); System.exit(1)
//      case Right(x) =>
//        for (partition <- x) {
//          topicAndPartitions += partition
//        }
//    }
//    //println("***************from kafka TopicAndPartition*******************")
//    //println(topicAndPartitions)
//    val consumerOffset: Either[KafkaCluster.Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)
//    consumerOffset match {
//      case Left(x) => System.err.println("kafka getConsumerOffsets error" + x); System.exit(1)
//      case Right(x) =>
//        x.foreach(
//          tp => {
//            fromOffsets += (tp._1 -> tp._2.offset)
//          }
//        )
//    }
//    fromOffsets
//  }
//
//  def getCurrentDate(message: String) {
//    var now: Date = new Date()
//    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    var hehe = dateFormat.format(now)
//    if (message.length() > 0) {
//      println("currentDate:" + message + ":" + hehe)
//    } else {
//      print("currentDate:" + hehe)
//    }
//  }
//
//  case class SrcLog(
//                     log_timestamp: Long = 0,
//                     ip: String = "",
//                     field: collection.Map[String, ExtValue] = null,
//                     thedate: String = "",
//                     thehour: String = "",
//                     theminute: String = ""
//                   )
//
//  case class ExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")
//
//  case class CoinBlackIdeaid(var ideaid:Int = 0,
//                             var show_num:Int = 0,
//                             var coin_show_num:Int = 0,
//                             var date:String = "",
//                             var hour:String = "")
//
//  case class CoinBlackIdeaidPb(var ideaid:Int = 0,
//                               var show_num:Int = 0,
//                               var coin_show_num:Int = 0,
//                               var coin_show_rate:Double = 0,
//                               var date:String = "",
//                               var hour:String = "",
//                               var minute:String = "")
//}
