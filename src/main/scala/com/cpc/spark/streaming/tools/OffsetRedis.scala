package com.cpc.spark.streaming.tools

import redis.clients.jedis.Jedis

import collection.JavaConversions
import collection.convert.wrapAsScala._
import collection.convert.wrapAsJava._
import java.util.Properties
import java.io.InputStream
import java.io.FileInputStream
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkFiles
class OffsetRedis extends Serializable {
   //private var jedis:Jedis = new Jedis("192.168.101.48", 6379)  
   
   private var key = "SRCLOG_KAFKA_OFFSET"
   def getRedis():Jedis = {
//     val prop = new Properties()
//     var in: InputStream = new FileInputStream(new File(SparkFiles.get("redis.properties")))
//         prop.load(in)
     val conf = ConfigFactory.load()
     var jedis:Jedis = new Jedis(conf.getString("redis.host"), conf.getInt("redis.port"))
     jedis
   }

  def setTopicAndPartitionOffSet(topic:String,partion:Int,offset:Long):Unit ={
     var jedis = getRedis()
     var long = jedis.set(key+"_"+topic+"_"+partion,offset.toString())
     jedis.sadd(key +"_"+ topic, partion.toString())
   }
   def getPartitionByTopic(topic:String):Set[String] ={
     var jedis = getRedis()
     val set:java.util.Set[String] =jedis.smembers(key+"_"+topic)// smembers返回java.util.Set[String]
     return set.toSet
   }
   def getTopicAndPartitionOffSet(topic:String,partion:Int):Long ={
     var jedis = getRedis()
     var offset = jedis.get(key+"_"+topic+"_"+partion)
     return offset.toLong     
   }
   def refreshOffsetKey():Unit ={
     var jedis = getRedis()
     val sets:java.util.Set[String]= jedis.keys(key+"*")
     val scalaSets = sets.toSet     
     for(key <- scalaSets){       
       jedis.del(key)
     }
   }

  def setTopicAndPartitionOffSet(key:String,topic:String,partion:Int,offset:Long):Unit ={
    var jedis = getRedis()
    var long = jedis.set(key+"_"+topic+"_"+partion,offset.toString())
    jedis.sadd(key +"_"+ topic, partion.toString())
  }
  def getPartitionByTopic(key:String,topic:String):Set[String] ={
    var jedis = getRedis()
    val set:java.util.Set[String] =jedis.smembers(key+"_"+topic)// smembers返回java.util.Set[String]
    return set.toSet
  }
  def getTopicAndPartitionOffSet(key:String,topic:String,partion:Int):Long ={
    var jedis = getRedis()
    var offset = jedis.get(key+"_"+topic+"_"+partion)
    return offset.toLong
  }
}

object OffsetRedis {
  var offsetRedis: OffsetRedis = _
  def getOffsetRedis: OffsetRedis = {
    synchronized {
      if (offsetRedis == null) {
        offsetRedis = new OffsetRedis()
      }
    }
    offsetRedis
  }
}




