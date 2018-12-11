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
  def saveExample2Hdfs(str: String, path: String, numPartitions: Int = 100): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    //str = "table_name/dt=2018-12-08,pt=daily,hour=00,ml_name=adcontent,ml_ver=v7"

    val table = str.split("/")(0)
    val condition = str.split("/")(1).replace("=", "='").replace(",", "' and ") + "'"

    val sql = s"select example from $table where $condition"
    print(sql)

    /*val data = spark.sql(sql)
      .rdd.map(x => Base64.decodeBase64(x.getString(0)))

    val path_exists = s"hadoop fs -test -e $path" !
    if (path_exists > 0) {
      s"hadoop fs -rm -r $path" !
    }

    data.map(x => (new BytesWritable(x), NullWritable.get()))
      .repartition(numPartitions)
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](path)*/
  }
}
