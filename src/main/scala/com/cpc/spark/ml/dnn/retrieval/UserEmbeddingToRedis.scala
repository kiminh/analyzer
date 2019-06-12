package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.Utils.CommonUtils
import mlmodel.mlmodel.RetrievalEmbedding
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{HostAndPort, JedisCluster}

object UserEmbeddingToRedis {
  val hdfsDir = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/user-output/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-user-embedding-upload")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val data = spark.read.parquet(hdfsDir + date)
    println("start write to redis")
    data.rdd.repartition(10).foreachPartition(
      iterator => {
        val jedis = new JedisCluster(new HostAndPort("192.168.83.62", 7001))
        iterator.foreach(x => {
          val uid = x.getAs[Array[Byte]](64).map(_.toChar).mkString
          val embedding = new Array[Double](64)

          for (i <- 0 to 63) {
            embedding(i) = x.getAs[Number](i).doubleValue()
          }
          val embPb = new RetrievalEmbedding(
            size = 64,
            embeddings = embedding
          )
          jedis.setex(("dssm-u-" + uid).getBytes(), 3600 * 24 * 30, embPb.toByteArray)
        })
        jedis.close()
      }
    )
  }
}