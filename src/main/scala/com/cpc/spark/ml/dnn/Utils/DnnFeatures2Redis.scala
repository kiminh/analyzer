package com.cpc.spark.ml.dnn.Utils

import com.cpc.spark.common.Murmur3Hash
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.DnnMultiHot
import org.apache.spark.sql.DataFrame

/**
  *
  * created time : 2018/11/29 11:24
  *
  * @author zhj
  * @version 1.0
  *
  */
object DnnFeatures2Redis {
  def multiHot2Redis(data: DataFrame, keyPrefix: String, key_type: String): Unit = {

    val conf = ConfigFactory.load()
    val col_length = data.columns.length
    val columns = data.columns
    println("column length : " + col_length)
    data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      p.foreach { rec =>
        var group = Seq[Int]()
        var hashcode = Seq[Long]()
        val key = keyPrefix + rec.get(0).toString
        for (i <- 1 until col_length) {
          val f = rec.getAs[Seq[Long]](i)
          group = group ++ Array.tabulate(f.length)(x => i - 1)
          hashcode = hashcode ++ f
        }
        redis.setex(key, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
      }

      redis.disconnect
    }

    println("----------------------------------------------")
    println(s"start setting default value for $keyPrefix ")
    var group = Seq[Int]()
    var hashcode = Seq[Long]()
    val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
    redis.auth(conf.getString("ali_redis.auth"))

    for (i <- 1 until col_length) {
      group = group ++ Seq(i - 1)
      hashcode = hashcode ++ Seq(Murmur3Hash.stringHash64(columns(i) + "#", 0))
    }

    redis.setex(keyPrefix + "default", 3600 * 24 * 2, DnnMultiHot(group, hashcode).toByteArray)
    redis.disconnect
  }

}
