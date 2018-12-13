package com.cpc.spark.ml.dnn.baseData

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
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
    val sql = generateSql(str, "example")
    print(sql)

    val path_exists = s"hadoop fs -test -e $path" !

    if (path_exists == 0) {

      s"hadoop fs -rm -r $path" !

    }

    spark.sql(sql)
      .repartition(numPartitions)
      .rdd.map(x => Base64.decodeBase64(x.getString(0)))
      .map(x => (new BytesWritable(x), NullWritable.get()))
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](path)
  }

  //保存multihot特征到redis
  def save2Redis(str: String, prefix: String): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val conf = ConfigFactory.load()

    val sql = generateSql(str, "redis")
    print(sql)

    spark.sql(sql).repartition(20)
      .rdd.map(x => (prefix + x.getString(0), Base64.decodeBase64(x.getString(0))))
      .foreachPartition {
        p => {
          val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
          redis.auth(conf.getString("ali_redis.auth"))
          p.foreach { rec =>
            redis.setex(rec._1, 3600 * 24 * 7, rec._2)
          }
          redis.disconnect
        }
      }
  }


  def generateSql(str: String, t: String): String = {
    val table = str.split("/")(0)
    val condition = str.split("/")(1).replace("\"", "").replace("'", "")
      .replace("=", "='").replace(",", "' and ") + "'"

    if (t == "example") s"select example from $table where $condition"
    else if (t == "redis") s"select key, dnnmultihot from $table where $condition"
    else ""
  }
}
