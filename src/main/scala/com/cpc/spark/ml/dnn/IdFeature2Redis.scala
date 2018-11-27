package com.cpc.spark.ml.dnn

import com.cpc.spark.common.Murmur3Hash
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import mlmodel.mlmodel.DnnMultiHot

/**
  * 生成广告title分词hash后文件供上线使用
  * ideaid
  * created time : 2018/11/27 14:01
  *
  * @author zhj
  * @version 1.0
  *
  */
object IdFeature2Redis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("hashSeq", hashSeq4Hive _)

    val ideaid_sql =
      """
        |select id as ideaid,
        |       hashSeq(split(tokens,' ')) as m16
        |from dl_cpc.ideaid_title
      """.stripMargin

    println("-----------------------------")
    println(ideaid_sql)
    println("-----------------------------")

    val data = spark.sql(ideaid_sql).persist()

    data.show(false)
    println("id features count : " + data.count)

    val conf = ConfigFactory.load()
    data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      p.foreach { rec =>
        var group = Seq[Int]()
        val ideaid = "ad_" + rec.getInt(0)
        val hashcode = rec.getAs[Seq[Long]](1)

        group = group ++ Array.tabulate(hashcode.length)(x => 0)

        redis.setex(ideaid, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
      }

      redis.disconnect
    }

  }

  def hashSeq4Hive(values: Seq[String]): Seq[Long] = {
    if (values.isEmpty) Seq(Murmur3Hash.stringHash64("m16", 0)) else
      for (v <- values) yield Murmur3Hash.stringHash64(v, 0)
  }
}
