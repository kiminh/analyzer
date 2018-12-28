package com.cpc.spark.ml.novel.retrieval

import com.redis.RedisClient
import mlmodel.mlmodel.RetrievalEmbedding
import org.apache.spark.sql.SparkSession

object UserEmbeddingToRedis {
  val hdfsDir = "/user/cpc/wy/novel/dssm/user-output/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-user-embedding-upload")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val data = spark.read.parquet(hdfsDir + date)
    data.rdd.repartition(10).foreachPartition(
      iterator => {
        val redis = new RedisClient("r-2ze5dd7d4f0c6364.redis.rds.aliyuncs.com", 6379)
        redis.auth("J9Q4wJTZbCk4McdiO8U5rIJW")
        redis.select(3)
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
          redis.setex("dssm-n-" + uid, 3600 * 24 * 30, embPb)
        })
        redis.disconnect
      }
    )
  }
}