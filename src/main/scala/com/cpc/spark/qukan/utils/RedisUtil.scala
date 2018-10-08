package com.cpc.spark.qukan.utils

import com.alibaba.fastjson.JSON
import com.cpc.spark.ml.train.Ftrl
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable

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
    for (key <- keySet) {
      // expire after 2 weeks
      nMap.put(key, redis.get[Double](s"n$key").getOrElse(0.0))
      zMap.put(key, redis.get[Double](s"z$key").getOrElse(0.0))
    }
    redis.disconnect
    return (nMap, zMap)
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
