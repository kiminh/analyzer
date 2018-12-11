package com.cpc.spark.ml.dnn.baseData

import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat
import sys.process._
import org.apache.spark.rdd.RDD

/**
  *
  * created time : 2018/12/11 16:30
  *
  * @author zhj
  * @version 1.0
  *
  */
object Utils {

  //写RDD[example]到hdfs
  def saveExample2Hdfs(data: RDD[Array[Byte]], path: String): Unit = {

    val path_exists = s"hadoop fs -test -e $path" !
    if (path_exists > 0) {
      s"hadoop fs -rm -r $path" !
    }

    data.map(x => (new BytesWritable(Base64.decodeBase64(x)), NullWritable.get()))
      .coalesce(1).saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](path)
  }
}
