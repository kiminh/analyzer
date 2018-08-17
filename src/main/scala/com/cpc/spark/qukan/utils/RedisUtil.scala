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

  def ftrlToRedis(ftrl: Ftrl, version: Int): Unit = {
    val redis = new RedisClient("192.168.80.20", 6390)
    redis.setex(s"ftrl-$version", 7 * 24 * 60 * 60, ftrl.toJsonString())
  }

  def redisToFtrl(version: Int): Ftrl = {
    val redis = new RedisClient("192.168.80.20", 6390)
    var res = redis.get[String](s"ftrl-$version")
    if (res == null || res.toString.trim == "" || res == None) {
      null
    } else {
      val jsonStr = res.get
//      println(s"jsonstr = $jsonStr")
      val json = JSON.parseObject(jsonStr)
      var ftrl = new Ftrl()
      ftrl.w = json.getString("w").split(" ").map(_.toDouble)
      ftrl.n = json.getString("n").split(" ").map(_.toDouble)
      ftrl.z = json.getString("z").split(" ").map(_.toDouble)
      ftrl.alpha = json.getDoubleValue("alpha")
      ftrl.beta = json.getDoubleValue("beta")
      ftrl.L1 = json.getDoubleValue("L1")
      ftrl.L2 = json.getDoubleValue("L2")
      ftrl
    }
  }

}
