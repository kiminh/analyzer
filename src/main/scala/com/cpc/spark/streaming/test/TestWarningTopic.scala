package com.cpc.spark.streaming.test


import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.KafkaConsumer


object TestWarningTopic {
  def main(args: Array[String]): Unit = {
//    val TOPIC = "cpc_realtime_parsedlog_warning"
//    val props = new Properties()
//    props.put("bootstrap.servers", "192.168.80.35:9092")
//    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    val consumer = new KafkaConsumer[String, String](props)
//    consumer.subscribe(Collections.singleton(TOPIC))
//    while(true) {
//      val records = consumer.poll(100)
//      for (record <- records.ass) {
//        println(record)
//      }
//    }

  }
}
