package com.cpc.spark.streaming.anal

import kafka.serializer.StringDecoder
import kafka.serializer.DefaultDecoder
import kafka.producer.KeyedMessage
import kafka.common.TopicAndPartition
import kafka.producer.KeyedMessage
import kafka.message.MessageAndMetadata
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.cpc.spark.common.LogData
import com.cpc.spark.common.Ui
import com.cpc.spark.common.Event
import com.cpc.spark.streaming.parser.StreamingDataParser
import org.apache.spark.rdd.RDD
import com.cpc.spark.streaming.tools.MDBManager
import com.mysql.jdbc.Statement
import data.Data
import com.cpc.spark.streaming.tools.OffsetRedis
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.TaskContext
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.log4j.{Level, Logger}

object CpcStreamingAnal {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println(s"""
        |Usage: CpcStreamingAnal <brokers> <topics> <out_topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <out_topics> is the out to kafka topic
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.INFO)
    val Array(brokers, topics, out_topics, seconds, repartition, env) = args
    
    val sparkConf = new SparkConf().setAppName("v2.9 cpc streaming anal topics = " + topics+ " env :" +env)
    val ssc = new StreamingContext(sparkConf, Seconds(seconds.toInt))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    
	  var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val messageHandler = (mmd: MessageAndMetadata[String, Array[Byte]]) => (mmd.topic, mmd.message())
    try {
      for(topic <- topicsSet){
        var partitions = OffsetRedis.getOffsetRedis.getPartitionByTopic(topic)
        for(partition <- partitions){
           println("topic:"+topic+";partition:" + partition)
           val tp = TopicAndPartition(topic, partition.toInt)         
           fromOffsets +=(tp ->  OffsetRedis.getOffsetRedis.getTopicAndPartitionOffSet(topic, partition.toInt))
        }
      }
     // OffsetRedis.refreshOffsetKey()
    } catch {
      case t: Exception =>  System.err.println("connect redis exception"+ t.getStackTrace)
      fromOffsets = Map()
    }
    println("***************redis fromOffsets*******************")     
    println(fromOffsets)  
    
    var kafkaoffset: Map[TopicAndPartition, Long] = null 
    
    if(!fromOffsets.isEmpty){
      kafkaoffset = getKafkaTopicAndPartitionOffset(topicsSet, kafkaParams)  
      println("***************from kafka offsets*******************")     
      println(kafkaoffset)
      kafkaoffset.foreach(tp => {        
         try {
           //可能key不存在
           if(!fromOffsets.isEmpty && fromOffsets.contains(tp._1)){
             if(fromOffsets(tp._1) < tp._2){
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
    
    if(kafkaoffset == null  || kafkaoffset.isEmpty){      
       messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topicsSet)
       println("no offset")
    }else{       
       messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, Array[Byte])](ssc, kafkaParams, kafkaoffset, messageHandler)
       println("from offset")
    }
    
    messages.foreachRDD { 
      rdd => {
         val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
         rdd.foreachPartition { iter =>
          try {
            val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)        
            OffsetRedis.getOffsetRedis.setTopicAndPartitionOffSet(o.topic, o.partition, o.fromOffset)        
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
          StreamingDataParser.streaming_data_parse(logdata)
        } catch {
          case t: Throwable => t.printStackTrace() // TODO: handle error
          (false, "", "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        }        
    }.filter {
      case (isok, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        isok
    }.map {
      case (isok, sid, date, hour, typed, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click) =>
        ((sid, typed), (date, hour, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) => x
    }.map {
      case ((sid, typed), (date, hour, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type, price, req, fill, imp, click)) => ((date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click))
    }.reduceByKey {
      case (x, y) =>
        (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5)
    }.map {
      case ((date, idea_id, unit_id, plan_id, user_id, media_id, adslot_id, adslot_type), (price, req, fill, imp, click)) =>
        (media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price)
    }
   // base_data.print()
    base_data.repartition(repartition.toInt).foreachRDD((x: RDD[(Int, Int, Int, Int, Int, Int, Int, String, Int, Int, Int, Int, Int)]) => {
      x.foreachPartition {
        data =>
          //getCurrentDate("start-conn")
          val startTime = System.currentTimeMillis()
          var sendSqlExcuteTime:Double = 0
          var sqlExcuteTime:Double = 0
          var kafakaSendTime:Double = 0
          val conn = MDBManager.getMDBManager(false).getConnection
          val conn2 = MDBManager.getMDB2Manager(false).getConnection
          conn.setAutoCommit(false)
          conn2.setAutoCommit(false)
          val stmt = conn.createStatement()
          val stmt2 = conn2.createStatement()
          val data_builder = Data.Log.newBuilder()
          val field_builder = Data.Log.Field.newBuilder()
          val map_builder = Data.Log.Field.Map.newBuilder()
          val valueType_builder = Data.ValueType.newBuilder()
          val ts = System.currentTimeMillis();
          var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
          var foreachCount = 0
          data.foreach {
            r =>
              foreachCount = foreachCount +1
              //`media_id`,`channel_id`,`adslot_id`,`adslot_type`,`idea_id`,`unit_id`,`plan_id`,`user_id`,`date`,`request`,`served_request`,`impression`,`click`,`activation`,`fee`
              val sql = "call eval_charge2(" + r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + ")"
              //              println("sql = " + sql)
              
              var reset:ResultSet = null
              val sendSqlStartTime = System.currentTimeMillis()
              if(r._6 >= 1000000 || r._6 == 0){
                 reset = stmt2.executeQuery(sql)
              }else{
                 reset = stmt.executeQuery(sql)
              }
              
              var store_sql_status = 2
              var consumeCoupon = 0
              var consumeBalance = 0
              var executeTime:Double = 0
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
              sendSqlExcuteTime += System.currentTimeMillis() - sendSqlStartTime
              data_builder.clear()
              field_builder.clear()
              map_builder.clear()
              valueType_builder.clear()
              //media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price
              data_builder.setLogTimestamp(ts)

              for (i <- 0 to r.productArity - 1) {
                valueType_builder.clear()
                map_builder.clear()
                if (i == 7) {
                  valueType_builder.setStringType(r.productElement(i).toString())
                } else {
                  valueType_builder.setIntType(r.productElement(i).toString().toInt)
                }
                val valueType = valueType_builder.build()
                val key = switchKeyById(i)
                map_builder.setKey(key)
                map_builder.setValue(valueType)
                field_builder.addMap(map_builder.build())
              }

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(0)
              map_builder.setKey("channel_id")
              map_builder.setValue(valueType_builder.build())
              map_builder.setKey("activation")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(store_sql_status)
              map_builder.setKey("sql_status")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(consumeBalance)
              map_builder.setKey("balance")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setIntType(consumeCoupon)
              map_builder.setKey("coupon")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())
              
              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setStringType(sql)
              map_builder.setKey("sql")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())

              valueType_builder.clear()
              valueType_builder.setStringType(r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + "," + consumeBalance + "," + consumeCoupon)
              map_builder.setKey("line")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())
              
              valueType_builder.clear()
              map_builder.clear()
              valueType_builder.setFloatType(executeTime.toFloat)
              map_builder.setKey("executeTime")
              map_builder.setValue(valueType_builder.build())
              field_builder.addMap(map_builder.build())
              
              data_builder.setField(field_builder.build())
              val message = new KeyedMessage[String, Array[Byte]](out_topics, null, data_builder.build().toByteArray())
              try {
                val startKafkaTime = System.currentTimeMillis()
                 producer.send(message)
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
          }

          conn.commit()
          conn.close()
          conn2.commit()
          conn2.close()
          producer.close()
          println(getCurrentDate("")+";timestart")
          println(getCurrentDate("")+";totalexcute-milliseconds:"+(System.currentTimeMillis() - startTime))
          println(getCurrentDate("")+";kafkaexcute-milliseconds:"+kafakaSendTime)
          println(getCurrentDate("")+";call-excute-milliseconds:"+sqlExcuteTime*1000)
          println(getCurrentDate("")+";sendsql-milliseconds:"+sendSqlExcuteTime)
          println(getCurrentDate("")+";foreach-count:"+foreachCount)
          println(getCurrentDate("")+";timeend")
          
      }
    })
     getCurrentDate("end-streaming")
    ssc.start()
    ssc.awaitTermination()
  }
  def getCurrentDate(message:String){
    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format( now )
    if(message.length() > 0){
      println("currentDate:"+message +":"+ hehe)
    }else{
      print("currentDate:"+ hehe)
    }
    
  }
  def switchKeyById(idx: Int): String = {
    var res: String = "None"
    //media_id, adslot_id, adslot_type, idea_id, unit_id, plan_id, user_id, date, req, fill, imp, click, price
    idx match {
      case 0  => res = "media_id";
      case 1  => res = "adslot_id";
      case 2  => res = "adslot_type";
      case 3  => res = "idea_id";
      case 4  => res = "unit_id";
      case 5  => res = "plan_id";
      case 6  => res = "user_id";
      case 7  => res = "date";
      case 8  => res = "req";
      case 9  => res = "fill";
      case 10 => res = "imp";
      case 11 => res = "click";
      case 12 => res = "price";
    }
    res
  }
 
  def getKafkaTopicAndPartitionOffset(topicsSet:Set[String], kafkaParams:Map[String, String]): Map[TopicAndPartition, Long] ={
    var kafkaCluster = new KafkaCluster(kafkaParams)     
    var topicAndPartitions = Set[TopicAndPartition]()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    val partitions:Either[KafkaCluster.Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(topicsSet)
    partitions match{
      case Left(x) => System.err.println("kafka getPartitions error" + x); System.exit(1)
      case Right(x) => {           
         for(partition <- x){
           topicAndPartitions += partition
         }
      }
    }
    println("***************from kafka TopicAndPartition*******************")     
    println(topicAndPartitions)    
    val consumerOffset:Either[KafkaCluster.Err, Map[TopicAndPartition, LeaderOffset]] = kafkaCluster.getLatestLeaderOffsets(topicAndPartitions)    
     consumerOffset match{
      case Left(x) => System.err.println("kafka getConsumerOffsets error" + x); System.exit(1)
      case Right(x) => {
        x.foreach(
            tp =>{                
                  fromOffsets += (tp._1 ->  tp._2.offset)  
                }
          )}
     }
    return fromOffsets
  }
}