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

  def ftrlToRedis(ftrl: Ftrl, version: Int): (Boolean, String) = {
    val redis = new RedisClient("192.168.80.20", 6390)
    val key = s"ftrl-$version"
    val success = redis.setex(key, 7 * 24 * 60 * 60, ftrl.toJsonString())
    redis.disconnect
    return (success, key)
  }

  def redisToFtrl(version: Int, featureSize: Int): Ftrl = {
    val redis = new RedisClient("192.168.80.20", 6390)
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

  def ftrlToRedisWithtype(ftrl: Ftrl, typename: String, version: Int, date: String, hour: String): Unit = {
    val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
    redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
    val key = s"ftrl-$typename-$version"
    val key2 = s"ftrl-$typename-$version-$date-$hour"
    println(s"key=$key, key2=$key2")
    redis.setex(key, 7 * 24 * 60 * 60, ftrl.toJsonString())
    redis.setex(key2, 7 * 24 * 60 * 60, ftrl.toJsonString())
    redis.disconnect
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
