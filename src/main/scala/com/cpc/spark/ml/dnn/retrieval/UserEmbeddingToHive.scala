package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.Utils.CommonUtils
import com.cpc.spark.ml.dnn.retrieval.DssmEvaluate.getUserEmbedding
import mlmodel.mlmodel.RetrievalEmbedding
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.{HostAndPort, JedisCluster}

object UserEmbeddingToHive {
  val hdfsDir = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/user-output/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-user-embedding-upload")
      .enableHiveSupport()
      .getOrCreate()

    val yesterday = args(0)

    import spark.implicits._

    val data = spark.read.parquet(hdfsDir + yesterday)

    val userDF=data.rdd.map(
      x => {
        val uid = x.getAs[Array[Byte]](64).map(_.toChar).mkString
        var result=(uid)
        val embedding = new Array[Double](64)
        for (i <- 0 to 63) {
          embedding(i) = x.getAs[Number](i).doubleValue()
          result=result+embedding(i)
        }
        result
      }
    ).toDF("uid","e0","e1","e10","e11","e12","e13","e14","e15","e16","e17","e18","e19",
      "e2","e20","e21","e22","e23","e24","e25","e26","e27","e28","e29",
      "e3","e30","e31","e32","e33","e34","e35","e36","e37","e38","e39",
      "e4","e40","e41","e42","e43","e44","e45","e46","e47","e48","e49",
      "e5","e50","e51","e52","e53","e54","e55","e56","e57","e58","e59",
      "e6","e60","e61","e62","e63",
      "e7","e8","e9")

    userDF.show(10)

    userDF.createOrReplaceTempView("user_embedding")

    spark.sql(s"""
                 |insert overwrite table dl_cpc.dssm_user_embedding_v3 partition (`dt`='$yesterday')
                 |select * from tmp_table
       """.stripMargin)
  }
}