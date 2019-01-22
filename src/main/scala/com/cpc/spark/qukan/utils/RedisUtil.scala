package com.cpc.spark.qukan.utils

import com.alibaba.fastjson.JSON
import com.cpc.spark.ml.train.Ftrl
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import redis.clients.jedis.{HostAndPort, JedisCluster}

object RedisUtil {

  def toRedis(dataset: Dataset[Row], key: String, value: String, prefix: String): Unit = {
    val data = dataset.select(key, value)
    data.foreachPartition(iterator => {

      val redisMap = mutable.LinkedHashMap[Int, RedisClient](
        0 -> new RedisClient("192.168.80.20", 6390),
        1 -> new RedisClient("192.168.80.27", 6390),
        2 -> new RedisClient("192.168.80.24", 6390),
        3 -> new RedisClient("192.168.80.28", 6390),
        4 -> new RedisClient("192.168.80.26", 6390),
        5 -> new RedisClient("192.168.80.25", 6390)
      )

      // record
      iterator.foreach(record => {
        val kValue = record.get(0).toString
        val vValue = record.get(1)
        val kk = s"$prefix$kValue"
        val id = (kk.toCharArray.map(_.toInt).sum) % 6;
        val redis = redisMap(id)
        redis.setex(kk, 7 * 24 * 60 * 60, vValue)
      })
      // close
      for ((k, v) <- redisMap) {
        v.disconnect
      }
    })
  }

  def fullModelToRedis(dbID: Int, w: Map[Int, Double], n: Map[Int, Double], z: Map[Int, Double]): Unit = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    redis.select(dbID)
    val wBuffer = ArrayBuffer[(Int, String)]()
    val nBuffer = ArrayBuffer[(String, String)]()
    val zBuffer = ArrayBuffer[(String, String)]()
    var counter = 0
    for ((key, value) <- w) {
      if (wBuffer.size >= 100) {
        redis.pipeline( p => {
          for ((key, value) <- wBuffer) {
            p.setex(key, 3600 * 24 * 5, value)
          }
          for ((key, value) <- nBuffer) {
            p.setex(key, 3600 * 24 * 5, value)
          }
          for ((key, value) <- zBuffer) {
            p.setex(key, 3600 * 24 * 5, value)
          }
        })
        counter += wBuffer.size
        wBuffer.clear()
        zBuffer.clear()
        nBuffer.clear()
      }
      wBuffer.append((key, value.toString))
      nBuffer.append((s"n$key", n.getOrElse(key, 0.0).toString))
      zBuffer.append((s"z$key", z.getOrElse(key, 0.0).toString))
    }
    redis.pipeline( p => {
      for ((key, value) <- wBuffer) {
        p.setex(key, 3600 * 24 * 5, value)
      }
      for ((key, value) <- nBuffer) {
        p.setex(key, 3600 * 24 * 5, value)
      }
      for ((key, value) <- zBuffer) {
        p.setex(key, 3600 * 24 * 5, value)
      }
    })
    counter += wBuffer.size
    println(s"Save $counter features to redis")
  }


  def getRedisClient(dbID: Int): RedisClient = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    redis.select(dbID)
    redis
  }

  def modelToRedis(dbID: Int, weights: Map[Int, Double]): Unit = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    redis.select(dbID)
    for ((key, value) <- weights) {
      // expire after 2 weeks
      redis.setex(key, 14 * 24 * 60 * 60, value.toString)
    }
    redis.disconnect
  }

  def modelToRedisWithPrefix(dbID: Int, weights: Map[Int, Double], prefix: String): Unit = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    redis.select(dbID)
    for ((key, value) <- weights) {
      // expire after 2 weeks
      redis.setex(s"$prefix$key", 14 * 24 * 60 * 60, value.toString)
    }
    redis.disconnect
  }

  def getNZFromRedis(dbID: Int, keySet : mutable.Set[Int]): (mutable.Map[Int, Double], mutable.Map[Int, Double])  = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    redis.select(dbID)
    val nMap = mutable.Map[Int, Double]()
    val zMap = mutable.Map[Int, Double]()
    val buffer = ArrayBuffer[Int]()
    var counter = 0
    for (key <- keySet) {
      if (buffer.size >= 100) {
        val nKeys = buffer.map(x => s"n$x")
        val nList = redis.mget[String](nKeys.head, nKeys.tail:_*).getOrElse(List())
        val zKeys = buffer.map(x => s"z$x")
        val zList = redis.mget[String](zKeys.head, zKeys.tail:_*).getOrElse(List())
        for (i <- buffer.indices) {
          if (nList.size > i) {
            val k = buffer(i)
            nMap.put(k, nList(i).getOrElse("0.0").toDouble)
            zMap.put(k, zList(i).getOrElse("0.0").toDouble)
            counter += 1
          }
        }
        buffer.clear()
      }
      buffer.append(key)
    }
    val nKeys = buffer.map(x => s"n$x")
    val nList = redis.mget[String](nKeys.head, nKeys.tail:_*).getOrElse(List())
    val zKeys = buffer.map(x => s"z$x")
    val zList = redis.mget[String](zKeys.head, zKeys.tail:_*).getOrElse(List())
    for (i <- buffer.indices) {
      if (nList.size > i) {
        val k = buffer(i)
        nMap.put(k, nList(i).getOrElse("0.0").toDouble)
        zMap.put(k, zList(i).getOrElse("0.0").toDouble)
        counter += 1
      }
    }
    println(s"total feature get: $counter")
    redis.disconnect
    (nMap, zMap)
  }

  def ftrlToRedis(ftrl: Ftrl, version: Int): (Boolean, String) = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    val key = s"ftrl-$version"
    val success = redis.setex(key, 7 * 24 * 60 * 60, ftrl.toJsonString)
    redis.disconnect
    return (success, key)
  }

  def redisToFtrl(version: Int, featureSize: Int): Ftrl = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    val res = redis.get[String](s"ftrl-$version")
    redis.disconnect
    if (res == null || res.toString.trim == "" || res == None) {
      null
    } else {
      val ftrl = new Ftrl(featureSize)
      ftrl.fromJsonString(res.get)
      ftrl
    }
  }

  def ftrlToRedisWithType(ftrl: Ftrl, typename: String, version: Int, date: String, hour: String): (String, String) = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    val key = s"ftrl-$typename-$version"
    val key2 = s"ftrl-$typename-$version-$date-$hour"
    println(s"key=$key, key2=$key2")
    redis.setex(key, 7 * 24 * 60 * 60, ftrl.toJsonString)
    redis.setex(key2, 7 * 24 * 60 * 60, ftrl.toJsonString)
    redis.disconnect
    return (key, key2)
  }

  def ftrlToRedisWithTypeV2(ftrl: Ftrl, typename: String, version: Int, date: String, hour: String): (String, String) = {
    /*
    新版redis数据
     */
    val jedis = new JedisCluster(new HostAndPort("192.168.83.62", 7001))
//    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
//    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    val key = s"ftrl-$typename-$version"
    val key2 = s"ftrl-$typename-$version-$date-$hour"
    println(s"key=$key, key2=$key2")
    jedis.setex(key, 7 * 24 * 60 * 60, ftrl.toJsonString)
    jedis.setex(key2, 7 * 24 * 60 * 60, ftrl.toJsonString)


    return (key, key2)
  }

  def redisToFtrlWithType(typename: String, version: Int, featureSize: Int): Ftrl = {

    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    var res = redis.get[String](s"ftrl-$typename-$version")
    redis.disconnect
    if (res == null || res.toString.trim == "" || res == None) {
      null
    } else {
      val ftrl = new Ftrl(featureSize)
      ftrl.fromJsonString(res.get)
      return ftrl
    }
  }

}
