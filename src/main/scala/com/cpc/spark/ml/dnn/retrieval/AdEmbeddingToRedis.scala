package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.Utils.CommonUtils
import mlmodel.mlmodel.RetrievalEmbedding
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{HostAndPort, JedisCluster}

object AdEmbeddingToRedis {
  val hdfsDir = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/ad-output/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-user-embedding-upload")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val data = spark.read.parquet(hdfsDir + date+"")
    println("start write to redis")
    data.rdd.repartition(100).foreachPartition(
      iterator => {
        val jedis = new JedisCluster(new HostAndPort("192.168.86.36", 7107))
        iterator.foreach(x => {
          val aid = x.getAs[Array[Byte]](0).map(_.toChar).mkString
          val embedding = new Array[Double](64)

          for (i <- 1 to 64) {
            embedding(i-1) = x.getAs[Number](i).doubleValue()
          }
          val embPb = new RetrievalEmbedding(
            size = 64,
            embeddings = embedding
          )
          jedis.setex(("dssm-a-" + aid).getBytes(), 3600 * 24 * 30, embPb.toByteArray)
        })
        jedis.close()
      }
    )
  }
}