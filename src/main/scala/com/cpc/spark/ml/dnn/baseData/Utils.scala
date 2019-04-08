package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.tensorflow.hadoop.io.TFRecordFileOutputFormat

import sys.process._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster}

import scala.util.Random

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
    println(sql)

    val path_exists = s"hadoop fs -test -e $path" !

    if (path_exists == 0) {

      s"hadoop fs -rm -r $path" !

    }

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    spark.sql(sql)
      .repartition(numPartitions)
      .rdd.map(x => Base64.decodeBase64(x.getString(0)))
      .filter(_ != null)
      .map(x => {
        acc.add(1L)
        (new BytesWritable(x), NullWritable.get())
      })
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](path)

    //保存count文件
    val fileName = "count_" + Random.nextInt(100000)
    println("count file name : " + fileName)
    println(s"total num is : ${acc.sum}")
    writeNum2File(fileName, acc.sum)

    s"hadoop fs -put $fileName $path/count" !

    val cnt = s"cat $fileName" !!

    if (cnt.stripLineEnd == "") {
      println("ERROR : there is no number in count file")
      System.exit(1)
    } else {
      println(s"the number in count file : ${cnt.stripLineEnd}")
    }
  }

  //写RDD[example]针对gauc样本到hdfs
  def saveGaucExample2Hdfs(str: String, path: String, numPartitions: Int = 100): Unit = {

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    //str = "table_name/dt=2018-12-08,pt=daily,hour=00,ml_name=adcontent,ml_ver=v7"
    val sql = generateSql(str, "gauc_example")
    println(sql)

    val path_exists = s"hadoop fs -test -e $path" !

    if (path_exists == 0) {

      s"hadoop fs -rm -r $path" !

    }

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    spark.sql(sql)
      .repartition(numPartitions, $"uid")
      .rdd.map(x => Base64.decodeBase64(x.getString(0)))
      .filter(_ != null)
      .map(x => {
        acc.add(1L)
        (new BytesWritable(x), NullWritable.get())
      })
      .saveAsNewAPIHadoopFile[TFRecordFileOutputFormat](path)

    //保存count文件
    val fileName = "count_" + Random.nextInt(100000)
    println("count file name : " + fileName)
    println(s"total num is : ${acc.sum}")
    writeNum2File(fileName, acc.sum)

    s"hadoop fs -put $fileName $path/count" !

    val cnt = s"cat $fileName" !!

    if (cnt.stripLineEnd == "") {
      println("ERROR : there is no number in count file")
      System.exit(1)
    } else {
      println(s"the number in count file : ${cnt.stripLineEnd}")
    }
  }

  //保存multihot特征到redis
  def save2Redis(str: String, prefix: String): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val conf = ConfigFactory.load()

    val sql = generateSql(str, "redis")
    print(sql)

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    spark.sql(sql).repartition(20)
      .rdd.map(x => (prefix + x.getString(0), Base64.decodeBase64(x.getString(1))))
      .foreachPartition {
        p => {
          //使用pipeline
          /*var i = 0
          val jedis = new Jedis(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
          jedis.auth(conf.getString("ali_redis.auth"))
          val pip = jedis.pipelined()
          p.foreach {
            rec =>
              pip.setex(rec._1, 3600 * 24 * 7, rec._2.toString)
              i += 1
              if (i % 100 == 0) {
                pip.sync()
              }
          }
          pip.sync()
          jedis.disconnect()*/

          val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
          redis.auth(conf.getString("ali_redis.auth"))
          p.foreach { rec =>
            val succ = redis.setex(rec._1, 3600 * 24 * 7, rec._2)
            if (succ) acc.add(0L) else acc.add(1L)
          }
          redis.disconnect
        }
      }
    val total_num = acc.count
    val fail_num = acc.sum
    val ratio = fail_num / total_num
    println(s"----- ali cloud -----")
    println(s"Total num = $total_num, Fail num = $fail_num, Fail ratio = $ratio")
    println("---------------------")
  }

  def save2RedisCluster(str: String, prefix: String): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sql = generateSql(str, "redis")
    print(sql)

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    spark.sql(sql).repartition(20)
      .rdd.map(x => (prefix + x.getString(0), Base64.decodeBase64(x.getString(1))))
      .foreachPartition {
        p => {
          val jedis = new JedisCluster(new HostAndPort("192.168.83.62", 7001))
          p.foreach { rec =>
            val re = jedis.setex(rec._1.getBytes(), 3600 * 24 * 7, rec._2)
            if (re == "OK") acc.add(0L) else acc.add(1L)
          }
          jedis.close()
        }
      }

    val total_num = acc.count
    val fail_num = acc.sum
    val ratio = fail_num / total_num
    println(s"----- cluster -----")
    println(s"Total num = $total_num, Fail num = $fail_num, Fail ratio = $ratio")
    println("---------------------")
  }

  def evalRedisVol(str: String, prefix: String): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val sql = generateSql(str, "redis")
    spark.sql(sql).sample(withReplacement = false, 0.01)
      .rdd.map(x => (prefix + x.getString(0), Base64.decodeBase64(x.getString(1))))
      .coalesce(10)
      .toDF("key", "value")
      .write.mode("overwrite")
      .parquet("/user/cpc/dnn/eval/redis/")

    val size = "hadoop fs -du -h /user/cpc/dnn/eval" #| "grep redis" !!

    val Array(num, unit, _*) = size.split(" ")

    println(s"the data of $str will take ${num.toFloat * 200} $unit volumn of redis ")


  }


  def generateSql(str: String, t: String): String = {
    val table = str.split("/")(0)
    val condition = str.split("/")(1).replace("\"", "").replace("'", "")
      .replace("=", "='").replace(",", "' and ") + "'"

    if (t == "example") s"select example from $table where $condition"
    else if (t == "redis") s"select key, dnnmultihot from $table where $condition"
    else if (t == "gauc_example") s"select example, uid from $table where $condition"
    else ""
  }

  def writeNum2File(file: String, num: Long): Unit = {
    val writer = new PrintWriter(new File(file))
    writer.write(num.toString)
    writer.close()
  }
}
