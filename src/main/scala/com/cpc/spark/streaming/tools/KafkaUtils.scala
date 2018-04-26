package com.cpc.spark.streaming.tools

import kafka.producer.{ KeyedMessage, Producer, ProducerConfig }
import java.util.Properties

object KafkaUtils {

  def getProducer(brokers: String): Producer[String, Array[Byte]] = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("value.serializer.class", "kafka.serializer.DefaultEncoder")
    props.put("key.serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    val producer = new Producer[String, Array[Byte]](config)
    producer
  }
}