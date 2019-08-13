package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.WrappedArray
import scala.sys.process._
import scala.util.Random

object RandomShuffle {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    import spark.implicits._

    val model_name = "adlist-v4-imp"
    val date = args(0)
    val path = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/$model_name/$date/part*"
    var data: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(path)

    println("data read finish")

    val data2 = data.rdd.repartition(1000).zipWithIndex().map{
      x=>
        val idx2 = x._1.getAs[WrappedArray[Long]]("idx2").toSeq
        val idx1 = x._1.getAs[WrappedArray[Long]]("idx1").toSeq
        val id_arr = x._1.getAs[WrappedArray[Long]]("id_arr").toSeq
        val idx0 = x._1.getAs[WrappedArray[Long]]("idx0").toSeq
        val dense = x._1.getAs[WrappedArray[Long]]("dense").toSeq
        val id = x._2.toLong
        (idx2,idx1,id_arr,idx0,dense,id)
    }.toDF("idx2","idx1","id_arr","idx0","dense","id").createOrReplaceTempView("feature_noise")

    val data3 = data.rdd.repartition(500).zipWithIndex().map{
      x=>
        val idx2 = x._1.getAs[WrappedArray[Long]]("idx2").toSeq
        val idx1 = x._1.getAs[WrappedArray[Long]]("idx1").toSeq
        val id_arr = x._1.getAs[WrappedArray[Long]]("id_arr").toSeq
        val idx0 = x._1.getAs[WrappedArray[Long]]("idx0").toSeq
        val sample_idx = x._1.getAs[Long]("sample_idx").toLong
        val label = x._1.getAs[WrappedArray[Long]]("label").toSeq
        val dense = x._1.getAs[WrappedArray[Long]]("dense").toSeq
        val id = x._2.toLong
        (idx2,idx1,id_arr,idx0,sample_idx,label,dense,id)
    }.toDF("idx2","idx1","id_arr","idx0","sample_idx","label","dense","id").createOrReplaceTempView("feature")

    println("sql begin")

    val data4 = spark.sql(
      s"""
      with t1 as (
        select * from feature_noise
      ),t2 as (
         select * from feature
      )
      select t2.idx2,t2.idx1,t2.id_arr,t2.idx0,t2.sample_idx,t2.label,t2.dense,
      t1.idx2 as idx2_noise,t1.idx1 as idx1_noise,t1.id_arr as id_arr_noise,t1.idx0 as idx0_noise,t1.dense as dense_noise
      from t2 join t1 on t1.id=t2.id
        """).toDF("idx2","idx1","id_arr","idx0","sample_idx","label","dense","idx2_noise","idx1_noise","id_arr_noise","idx0_noise","dense_noise")
    val path2 = s"hdfs://emr-cluster/user/cpc/xy/feature-important/$model_name/$date/"
    data4.write.format("tfrecords").option("recordType", "Example").save(path2)
    println("to tfrecord finish")
    println("data num"+data4.count().toString())


    val fileName = "count_" + Random.nextInt(100000)
    println("count file name : " + fileName)
    writeNum2File(fileName, data4.count())
    s"hadoop fs -put $fileName $path2/count" !

    s"hadoop fs -chmod 777 hdfs://emr-cluster/user/cpc/xy/feature-important/$model_name/$date/*" !

  }

  def writeNum2File(file: String, num: Long): Unit = {
    val writer = new PrintWriter(new File(file))
    writer.write(num.toString)
    writer.close()
  }
}

//hadoop fs -put count hdfs://emr-cluster/user/cpc/novel/aiclk_dataflow/gpu/adlist-v4/2019-07-16/count  391072312
//hadoop fs -chmod 777 hdfs://emr-cluster/user/cpc/novel/aiclk_dataflow/gpu/adlist-v4/2019-07-15
//hadoop fs -chmod 777 hdfs://emr-cluster/user/cpc/novel/aiclk_dataflow/gpu/adlist-v4/2019-07-15/*