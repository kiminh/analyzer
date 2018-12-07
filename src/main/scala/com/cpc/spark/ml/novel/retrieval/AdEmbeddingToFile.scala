package com.cpc.spark.ml.novel.retrieval

import java.io.{File, FileOutputStream, PrintWriter}

import mlmodel.mlmodel.{AllAdEmbeddings, RetrievalEmbedding}
import org.apache.spark.sql.SparkSession
import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory

import scala.collection.mutable

/**
  * author: huazhenhao
  * date: 11/14/18
  */
object AdEmbeddingToFile {
  val localDir = "/home/cpc/scheduled_job/ad_embedding/"
  val destDir = "/home/cpc/model_server/data/"
  val hdfsDir = "/user/cpc/hzh/dssm/ad-output/"
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-ad-embedding-upload")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val data = spark.read.parquet(hdfsDir + date)
    val mm = mutable.Map[Int, RetrievalEmbedding]()
    data.rdd.toLocalIterator.foreach(
      row => {
          val ideaid = row.getAs[Array[Byte]](64).map(_.toChar).mkString.toInt
          val embedding = new Array[Double](64)

          for (i <- 0 to 63) {
            embedding(i) = row.getAs[Number](i).doubleValue()
          }
          val embPb = new RetrievalEmbedding(
            size = 64,
            embeddings = embedding
          )
          mm.put(ideaid, embPb)
        }
    )
    val adEmbedding = new AllAdEmbeddings(
      version = "v0",
      allEmbeddings = mm.toMap
    )
    val filename = s"ad_embeddings.pb"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    adEmbedding.writeTo(new FileOutputStream(localPath))
    println(Utils.updateMlcppModelData(localPath, destDir + filename, ConfigFactory.load()))
  }
}
