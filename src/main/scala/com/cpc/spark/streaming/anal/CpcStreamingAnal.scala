package com.cpc.spark.streaming.anal

import com.cpc.spark.common.LogData
import com.cpc.spark.streaming.parser.StreamingDataParser
import com.cpc.spark.streaming.tools.MDBManager
import data.Data
import kafka.producer.KeyedMessage
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object CpcStreamingAnal {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: CpcStreamingAnal <brokers> <topics> <out_topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <out_topics> is the out to kafka topic
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, out_topics) = args
    val sparkConf = new SparkConf().setAppName("cpc streaming anal topics = " + topics)
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](
      ssc, kafkaParams, topicsSet)

    val base_data = messages.map {
      case (k, v) =>
        val logdata = LogData.parseData(v)
        StreamingDataParser.streaming_data_parse(logdata)
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
    base_data.print()
    base_data.foreachRDD((x: RDD[(Int, Int, Int, Int, Int, Int, Int, String, Int, Int, Int, Int, Int)]) => {
      x.foreachPartition {
        data =>
          val conn = MDBManager.getMDBManager(false).getConnection
          conn.setAutoCommit(false)
          val stmt = conn.createStatement()
          val data_builder = Data.Log.newBuilder()
          val field_builder = Data.Log.Field.newBuilder()
          val map_builder = Data.Log.Field.Map.newBuilder()
          val valueType_builder = Data.ValueType.newBuilder()
          val ts = System.currentTimeMillis();
          var producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
          data.foreach {
            r =>
              //`media_id`,`channel_id`,`adslot_id`,`adslot_type`,`idea_id`,`unit_id`,`plan_id`,`user_id`,`date`,`request`,`served_request`,`impression`,`click`,`activation`,`fee`
              val sql = "call eval_charge2(" + r._1 + ",0," + r._2 + "," + r._3 + "," + r._4 + "," + r._5 + "," + r._6 + "," + r._7 + ",'" + r._8 + "'" + "," + r._9 + "," + r._10 + "," + r._11 + "," + r._12 + ",0," + r._13 + ")"
              //              println("sql = " + sql)
              val reset = stmt.executeQuery(sql)
              var store_sql_status = 2
              var consumeCoupon = 0
              var consumeBalance = 0
              while (reset.next()) {
                try {
                  store_sql_status = reset.getInt(1)
                  consumeBalance = reset.getInt(2)
                  consumeCoupon = reset.getInt(3)
                } catch {
                  case ex: Exception =>
                    ex.printStackTrace()
                    store_sql_status = 3
                }
              }

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
              
              data_builder.setField(field_builder.build())
              val message = new KeyedMessage[String, Array[Byte]](out_topics, null, data_builder.build().toByteArray())
              try {
                producer.send(message)
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
          producer.close()
      }
    })
    ssc.start()
    ssc.awaitTermination()
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
}