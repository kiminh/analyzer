package com.cpc.spark.ml.dnn.Utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CommonUtils {
  val HDFS_PREFIX_PATH="hdfs://emr-cluster/"


  def writeCountToFile(spark: SparkSession, count: Long, trainCountPathTmpName: String, trainCountPathName: String) = {
    val arr = Array(count)
    val rdd = spark.sparkContext.parallelize(arr).repartition(1)

    CommonUtils.rddWriteFile(spark, trainCountPathTmpName, trainCountPathName, rdd)
  }

  def rddWriteFile(spark: SparkSession, tmpOutputPath: String, outputPath: String, rdd: RDD[Long]) = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val hdfs = FileSystem.get(hadoopConf)

    val trainCountPath = new Path(tmpOutputPath)
    if (hdfs.exists(trainCountPath)) {
      hdfs.delete(trainCountPath, true)
    }

    rdd.saveAsTextFile(tmpOutputPath)

    if (hdfs.exists(new Path(outputPath))) {
      hdfs.delete(new Path(outputPath), true)
    }

    hdfs.rename(new Path(tmpOutputPath + "part-00000"), new Path(outputPath))
  }
}
