package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.sys.process._
import scala.util.Random

/**
  * 解析adlistv4tfrecord特征
  * created time : 2019/09/19 23:31
  * @author fenghuabin
  * @version 1.0
  *
  */

object CollectIncData {


  def delete_hdfs_path(path: String): Unit = {

    val conf = new org.apache.hadoop.conf.Configuration()
    val p = new org.apache.hadoop.fs.Path(path)
    val hdfs = p.getFileSystem(conf)
    val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)

    //val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    if (hdfs.exists(hdfs_path)) {
      hdfs.delete(hdfs_path, true)
    }
  }

  def exists_hdfs_path(path: String): Boolean = {

    val conf = new org.apache.hadoop.conf.Configuration()
    val p = new org.apache.hadoop.fs.Path(path)
    val hdfs = p.getFileSystem(conf)
    val hdfs_path = new org.apache.hadoop.fs.Path(path.toString)
    //val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())

    if (hdfs.exists(hdfs_path)) {
      true
    } else {
      false
    }
  }

  def writeNum2File(file: String, num: Long): Unit = {
    val writer = new PrintWriter(new File(file))
    writer.write(num.toString)
    writer.close()
  }

  def formatDate(date: Date, pattern: String = "yyyy-MM-dd"): String = {
    val formatDate = DateFormatUtils.format(date, pattern)
    formatDate
  }

  def GetDataRangeWithWeek(beginStr: String, endStr: String, format: String = "yyyy-MM-dd"): ArrayBuffer[String] = {
    val ranges = ArrayBuffer[String]()
    val sdf = new SimpleDateFormat(format)
    var dateBegin = sdf.parse(beginStr)
    val dateEnd = sdf.parse(endStr)
    while (dateBegin.compareTo(dateEnd) <= 0) {
      ranges += sdf.format(dateBegin) + ";" + DateFormatUtils.format(dateBegin, "E")
      dateBegin = DateUtils.addDays(dateBegin, 1)
    }
    ranges
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      System.err.println(
        """
          |you have to input 9 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(des_dir, train_files_collect_8, train_files_collect_4, train_files_collect_2, train_files_collect_1, last_date, curr_date, time_id, delete_old) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext


    val bid_mmh_map_file = des_dir + "/" + "bid_mmh_map.txt"

    val bid_mmh_map = sc.textFile(bid_mmh_map_file).map({
      rs =>
        val line_list = rs.split("\t")
        (line_list(0), line_list(1))
    }).collectAsMap()
    println("bid_mmh_map.size=" + bid_mmh_map.size)


    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    val file_collect_8 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-collect-8"
    val file_collect_4 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-collect-4"
    val file_collect_2 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-collect-2"
    val file_collect_1 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-collect-1"
    val instances_8 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-instances-8"
    val instances_4 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-instances-4"
    val instances_2 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-instances-2"
    val instances_1 = des_dir + "/" + curr_date + "-collect-inc/" + curr_date + "-" + time_id + "-instances-1"
    val ctr_file = des_dir + "/" + curr_date + "-weight-collect-inc/" + curr_date + "-" + time_id + "-ctr"

    if (delete_old == "true") {
      delete_hdfs_path(file_collect_8)
      delete_hdfs_path(file_collect_4)
      delete_hdfs_path(file_collect_2)
      delete_hdfs_path(file_collect_1)
      delete_hdfs_path(file_collect_8)
      delete_hdfs_path(file_collect_4)
      delete_hdfs_path(file_collect_2)
      delete_hdfs_path(file_collect_1)
      delete_hdfs_path(ctr_file)
    }

    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))
    val low_time_list = "0001 3001 0002 3002 0003 3003 0004 3004 0005 3005".split(" ")

    /****************************************collect_1***************************************************/
    val df_train_files_collect_1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_1)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_1.printSchema()
    df_train_files_collect_1.show(3)

    df_train_files_collect_1.rdd.map(
      rs => {
        val idx_arr = rs.getSeq[Long](2)
        val dense = rs.getSeq[Long](6)

        val output: Array[String] = new Array[String](dense.length + idx_arr.length)
        for (idx <- dense.indices) {
          output(idx) = dense(idx).toString
        }
        for (idx <- idx_arr.indices) {
          output(idx + dense.length) = idx_arr(idx).toString
        }
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).map {
      case (key, value) =>
        key + "\t" + value.toString
    }.repartition(1).saveAsTextFile(instances_1)

    df_train_files_collect_1.rdd.map(
      rs => {
        val label_arr = rs.getSeq[Long](5)
        if (label_arr.head == 1L) {
          ("ctr", (1.0, 1.0))
        } else{
          ("ctr", (0.0, 1.0))
        }
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
      rs =>
        rs._1 + "\t" + rs._2._1 + "\t" + rs._2._2 + "\t" + rs._2._1/rs._2._2
    }).repartition(1).saveAsTextFile(ctr_file)

    val rdd_1 = df_train_files_collect_1.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

    val rdd_count_1 = rdd_1.count()
    println(s"rdd_count is : $rdd_count_1")
    println("DF file count:" + rdd_count_1.toString + " of file:" + train_files_collect_1)

    val tf_df_1: DataFrame = spark.createDataFrame(rdd_1, schema_new)
    tf_df_1.repartition(600).write.format("tfrecords").option("recordType", "Example").save(file_collect_1)

    //保存count文件
    val fileName_1 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_1, rdd_count_1)

    if (!low_time_list.contains(time_id) && rdd_count_1 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $rdd_count_1 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_1 $file_collect_1/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_1 $file_collect_1/count" !
    }
    s"hadoop fs -chmod -R 0777 $file_collect_1" !

    /****************************************collect_4***************************************************/
    val df_train_files_collect_4: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_4)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_4.printSchema()
    df_train_files_collect_4.show(3)

    df_train_files_collect_4.rdd.map(
      rs => {
        val idx_arr = rs.getSeq[Long](2)
        val dense = rs.getSeq[Long](6)

        val output: Array[String] = new Array[String](dense.length + idx_arr.length)
        for (idx <- dense.indices) {
          output(idx) = dense(idx).toString
        }
        for (idx <- idx_arr.indices) {
          output(idx + dense.length) = idx_arr(idx).toString
        }
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).map {
      case (key, value) =>
        key + "\t" + value.toString
    }.repartition(1).saveAsTextFile(instances_4)

    val rdd_4 = df_train_files_collect_4.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

    val rdd_count_4 = rdd_4.count()
    println(s"rdd_count is : $rdd_count_4")
    println("DF file count:" + rdd_count_4.toString + " of file:" + train_files_collect_4)

    val tf_df_4: DataFrame = spark.createDataFrame(rdd_4, schema_new)
    tf_df_4.repartition(600).write.format("tfrecords").option("recordType", "Example").save(file_collect_4)

    //保存count文件
    val fileName_4 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_4, rdd_count_4)

    if (!low_time_list.contains(time_id) && rdd_count_4 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $rdd_count_4 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_4 $file_collect_4/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_4 $file_collect_4/count" !
    }
    s"hadoop fs -chmod -R 0777 $file_collect_4" !

    /****************************************collect_2***************************************************/
    val df_train_files_collect_2: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_2)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_2.printSchema()
    df_train_files_collect_2.show(3)

    df_train_files_collect_2.rdd.map(
      rs => {
        val idx_arr = rs.getSeq[Long](2)
        val dense = rs.getSeq[Long](6)

        val output: Array[String] = new Array[String](dense.length + idx_arr.length)
        for (idx <- dense.indices) {
          output(idx) = dense(idx).toString
        }
        for (idx <- idx_arr.indices) {
          output(idx + dense.length) = idx_arr(idx).toString
        }
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).map {
      case (key, value) =>
        key + "\t" + value.toString
    }.repartition(1).saveAsTextFile(instances_2)

    val rdd_2 = df_train_files_collect_2.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

    val rdd_count_2 = rdd_2.count()
    println(s"rdd_count is : $rdd_count_2")
    println("DF file count:" + rdd_count_2.toString + " of file:" + train_files_collect_2)

    val tf_df_2: DataFrame = spark.createDataFrame(rdd_2, schema_new)
    tf_df_2.repartition(600).write.format("tfrecords").option("recordType", "Example").save(file_collect_2)

    //保存count文件
    val fileName_2 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_2, rdd_count_2)

    if (!low_time_list.contains(time_id) && rdd_count_2 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $rdd_count_2 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_2 $file_collect_2/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_2 $file_collect_2/count" !
    }
    s"hadoop fs -chmod -R 0777 $file_collect_2" !

    /****************************************collect_8***************************************************/
    val df_train_files_collect_8: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_8)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_8.printSchema()
    df_train_files_collect_8.show(3)

    df_train_files_collect_8.rdd.map(
      rs => {
        val idx_arr = rs.getSeq[Long](2)
        val dense = rs.getSeq[Long](6)

        val output: Array[String] = new Array[String](dense.length + idx_arr.length)
        for (idx <- dense.indices) {
          output(idx) = dense(idx).toString
        }
        for (idx <- idx_arr.indices) {
          output(idx + dense.length) = idx_arr(idx).toString
        }
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).map {
      case (key, value) =>
        key + "\t" + value.toString
    }.repartition(1).saveAsTextFile(instances_8)

    val rdd_8 = df_train_files_collect_8.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
      })

    val rdd_count_8 = rdd_8.count()
    println(s"rdd_count is : $rdd_count_8")
    println("DF file count:" + rdd_count_8.toString + " of file:" + train_files_collect_8)

    val tf_df_8: DataFrame = spark.createDataFrame(rdd_8, schema_new)
    tf_df_8.repartition(600).write.format("tfrecords").option("recordType", "Example").save(file_collect_8)

    //保存count文件
    val fileName_8 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_8, rdd_count_8)

    if (!low_time_list.contains(time_id) && rdd_count_8 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $rdd_count_8 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_8 $file_collect_8/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_8 $file_collect_8/count" !
    }
    s"hadoop fs -chmod -R 0777 $file_collect_8" !

  }
}

