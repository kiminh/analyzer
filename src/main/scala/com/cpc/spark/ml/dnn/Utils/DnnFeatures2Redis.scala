package com.cpc.spark.ml.dnn.Utils

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
    data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      key_type match {
        case "string" => {
          p.foreach { rec =>
            var group = Seq[Int]()
            var hashcode = Seq[Long]()
            val key = keyPrefix + rec.getString(0).toString
            for (i <- 1 until data.columns.length) {
              val f = rec.getAs[Seq[Long]](i)
              group = group ++ Array.tabulate(f.length)(x => i)
              hashcode = hashcode ++ f
            }
            redis.setex(key, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
          }
        }
        case "int" => {
          p.foreach { rec =>
            var group = Seq[Int]()
            var hashcode = Seq[Long]()
            val key = keyPrefix + rec.getInt(0).toString
            for (i <- 1 until data.columns.length) {
              val f = rec.getAs[Seq[Long]](i)
              group = group ++ Array.tabulate(f.length)(x => i)
              hashcode = hashcode ++ f
            }
            redis.setex(key, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
          }
        }
        case _ => {
          println("key type error ")
          System.exit(1)
        }
      }


      redis.disconnect
    }
  }
}
