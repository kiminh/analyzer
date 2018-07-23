package com.cpc.spark.streaming.tools

import data.Data
import kafka.producer.KeyedMessage


class Data2Kafka {
  val logBuilder = Data.Log.newBuilder()
  val fieldBuilder = Data.Log.Field.newBuilder()
  val mapBuilder = Data.Log.Field.Map.newBuilder()
  val valueBuilder = Data.ValueType.newBuilder()
  var producer: kafka.producer.Producer[String, Array[Byte]] = null

  def clear(): Unit = {
    logBuilder.clear()
    fieldBuilder.clear()
    mapBuilder.clear()
    valueBuilder.clear()
  }

  def setMessage(ts: Long, mapInt: Seq[(String, Int)] = null, mapFloat: Seq[(String, Float)] = null,
                 mapLong: Seq[(String, Long)] = null, mapString: Seq[(String, String)] = null): Data.Log.Builder = {
    clear()
    if (mapInt != null) {
      for (i <- mapInt) {
        valueBuilder.clear()
        mapBuilder.clear()
        valueBuilder.setIntType(i._2)
        mapBuilder.setKey(i._1)
        mapBuilder.setValue(valueBuilder.build())
        fieldBuilder.addMap(mapBuilder.build())
      }
    }
    if (mapFloat != null) {
      for (i <- mapFloat) {
        valueBuilder.clear()
        mapBuilder.clear()
        valueBuilder.setFloatType(i._2)
        mapBuilder.setKey(i._1)
        mapBuilder.setValue(valueBuilder.build())
        fieldBuilder.addMap(mapBuilder.build())
      }
    }
    if (mapLong != null) {
      for (i <- mapLong) {
        valueBuilder.clear()
        mapBuilder.clear()
        valueBuilder.setLongType(i._2)
        mapBuilder.setKey(i._1)
        mapBuilder.setValue(valueBuilder.build())
        fieldBuilder.addMap(mapBuilder.build())
      }
    }
    if (mapString != null) {
      for (i <- mapString) {
        valueBuilder.clear()
        mapBuilder.clear()
        valueBuilder.setStringType(i._2)
        mapBuilder.setKey(i._1)
        mapBuilder.setValue(valueBuilder.build())
        fieldBuilder.addMap(mapBuilder.build())
      }
    }
    logBuilder.setLogTimestamp(ts)
    logBuilder.setField(fieldBuilder.build())
  }

  def sendMessage(brokers: String, topic: String): Unit = {
    producer = com.cpc.spark.streaming.tools.KafkaUtils.getProducer(brokers)
    val message = new KeyedMessage[String, Array[Byte]](topic, logBuilder.build().toByteArray)
    producer.send(message)
  }

  def close(): Unit = {
    if (producer != null) {
      producer.close()
    }

  }
}
