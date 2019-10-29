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

object CollectIncWeightData {


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

    val weighted_file_collect_8 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-8"
    val weighted_file_collect_4 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-4"
    val weighted_file_collect_2 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-2"
    val weighted_file_collect_1 = des_dir + "/" + curr_date + "-" + time_id + "-weighted-collect-1"
    val ctr_file = des_dir + "/" + curr_date + "-" + time_id + "-ctr"

    if (delete_old == "true") {
      delete_hdfs_path(weighted_file_collect_8)
      delete_hdfs_path(weighted_file_collect_4)
      delete_hdfs_path(weighted_file_collect_2)
      delete_hdfs_path(weighted_file_collect_1)
      delete_hdfs_path(ctr_file)
    }

    val base_daily_weight_map_file = des_dir + s"/$last_date-weight-map"
    val weight_map_rdd = sc.textFile(base_daily_weight_map_file).map({
      rs =>
        //(ideal_id, bid_hash, bid_ori, weight_new_norm, weight_new_ori, weight, ctr, click, imp)
        val line_list = rs.split("\t")
        (line_list(0), line_list(1), line_list(3).toFloat)
    })

    val weight_map = weight_map_rdd.map({
      rs =>
        (rs._1 + "\t" + rs._2, rs._3)
    }).collectAsMap()
    val weight_map_ori = weight_map_rdd.map({
      rs =>
        (rs._1, rs._3)
    }).collectAsMap()


    println("weight_map.size=" + weight_map.size)
    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("weight", FloatType, nullable = true),
      StructField("weight_reverse", FloatType, nullable = true),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))
    val low_time_list = "0001 3001 0002 3002 0003 3003 0004 3004 0005 3005".split(" ")

    /****************************************collect_2***************************************************/
    val df_train_files_collect_2: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_2)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_2.printSchema()
    df_train_files_collect_2.show(3)

    val new_ideal_id_map_2 = df_train_files_collect_2.rdd.map(
      rs => {
        val dense = rs.getSeq[Long](6)
        val ideal_id = dense(11).toString
        if (weight_map_ori.contains(ideal_id)) {
          (ideal_id, false)
        } else {
          (ideal_id, true)
        }
      }).filter(rs => rs._2).map({rs => (rs._1, 1.0)}).reduceByKey(_ + _).collectAsMap()
    println("collect_2")
    println("last date ideal id count:" + weight_map.size)
    println("new ideal id count:" + new_ideal_id_map_2.size)
    println("new rate:" + (new_ideal_id_map_2.size + 0.0) / weight_map.size)

    val weighted_rdd_2 = df_train_files_collect_2.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        val ideal_id = dense(11).toString

        var weight = 1.0

        if (weight_map.contains(ideal_id + "\t" + bid)) {
          weight = weight_map(ideal_id + "\t" + bid)
        } else {
          if (weight_map_ori.contains(ideal_id)) {
            weight = weight_map_ori(ideal_id)
          } else {
            weight = 3.0
          }
        }

        val weight_reverse = 1.0/weight

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

    val weighted_rdd_count_2 = weighted_rdd_2.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_2")
    println("DF file count:" + weighted_rdd_count_2.toString + " of file:" + train_files_collect_2)

    val tf_df_2: DataFrame = spark.createDataFrame(weighted_rdd_2, schema_new)
    tf_df_2.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_2)

    //保存count文件
    val fileName_2 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_2, weighted_rdd_count_2)


    if (!low_time_list.contains(time_id) && weighted_rdd_count_2 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $weighted_rdd_count_2 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_2 $weighted_file_collect_2/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_2 $weighted_file_collect_2/count" !
    }

    s"hadoop fs -chmod -R 0777 $weighted_file_collect_2" !


    /****************************************collect_1***************************************************/
    val df_train_files_collect_1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_1)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_1.printSchema()
    df_train_files_collect_1.show(3)

    val new_ideal_id_map_1 = df_train_files_collect_1.rdd.map(
      rs => {
        val dense = rs.getSeq[Long](6)
        val ideal_id = dense(11).toString
        if (weight_map_ori.contains(ideal_id)) {
          (ideal_id, false)
        } else {
          (ideal_id, true)
        }
      }).filter(rs => rs._2).map({rs => (rs._1, 1.0)}).reduceByKey(_ + _).collectAsMap()
    println("collect_1")
    println("last date ideal id count:" + weight_map.size)
    println("new ideal id count:" + new_ideal_id_map_1.size)
    println("new rate:" + (new_ideal_id_map_1.size + 0.0) / weight_map.size)

    val weighted_rdd_1 = df_train_files_collect_1.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        val ideal_id = dense(11).toString

        var weight = 1.0

        if (weight_map.contains(ideal_id + "\t" + bid)) {
          weight = weight_map(ideal_id + "\t" + bid)
        } else {
          if (weight_map_ori.contains(ideal_id)) {
            weight = weight_map_ori(ideal_id)
          } else {
            weight = 3.0
          }
        }

        val weight_reverse = 1.0/weight

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

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

    val weighted_rdd_count_1 = weighted_rdd_1.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_1")
    println("DF file count:" + weighted_rdd_count_1.toString + " of file:" + train_files_collect_1)

    val tf_df_1: DataFrame = spark.createDataFrame(weighted_rdd_1, schema_new)
    tf_df_1.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_1)

    //保存count文件
    val fileName_1 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_1, weighted_rdd_count_1)

    if (!low_time_list.contains(time_id) && weighted_rdd_count_1 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $weighted_rdd_count_1 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_1 $weighted_file_collect_1/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_1 $weighted_file_collect_1/count" !
    }


    s"hadoop fs -chmod -R 0777 $weighted_file_collect_1" !

    /****************************************collect_4***************************************************/
    val df_train_files_collect_4: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_4)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_4.printSchema()
    df_train_files_collect_4.show(3)

    val new_ideal_id_map_4 = df_train_files_collect_4.rdd.map(
      rs => {
        val dense = rs.getSeq[Long](6)
        val ideal_id = dense(11).toString
        if (weight_map_ori.contains(ideal_id)) {
          (ideal_id, false)
        } else {
          (ideal_id, true)
        }
      }).filter(rs => rs._2).map({rs => (rs._1, 1.0)}).reduceByKey(_ + _).collectAsMap()
    println("collect_4")
    println("last date ideal id count:" + weight_map.size)
    println("new ideal id count:" + new_ideal_id_map_4.size)
    println("new rate:" + (new_ideal_id_map_4.size + 0.0) / weight_map.size)

    val weighted_rdd_4 = df_train_files_collect_4.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        val ideal_id = dense(11).toString

        var weight = 1.0

        if (weight_map.contains(ideal_id + "\t" + bid)) {
          weight = weight_map(ideal_id + "\t" + bid)
        } else {
          if (weight_map_ori.contains(ideal_id)) {
            weight = weight_map_ori(ideal_id)
          } else {
            weight = 3.0
          }
        }

        val weight_reverse = 1.0/weight

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

    val weighted_rdd_count_4 = weighted_rdd_4.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_4")
    println("DF file count:" + weighted_rdd_count_4.toString + " of file:" + train_files_collect_4)

    val tf_df_4: DataFrame = spark.createDataFrame(weighted_rdd_4, schema_new)
    tf_df_4.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_4)

    //保存count文件
    val fileName_4 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_4, weighted_rdd_count_4)


    if (!low_time_list.contains(time_id) && weighted_rdd_count_4 <= 5000000) {
      println(s"time_id $time_id not in low_time_list but count $weighted_rdd_count_4 less than 5 millions, invalid count")
      s"hadoop fs -put $fileName_4 $weighted_file_collect_4/invalid_count" !
    } else {
      s"hadoop fs -put $fileName_4 $weighted_file_collect_4/count" !
    }

    s"hadoop fs -chmod -R 0777 $weighted_file_collect_4" !

    /****************************************ctr_file***************************************************/
    /**val train_ids_list = train_ids.split(",")
    val train_files_list = train_files.split(",")
    if (train_ids_list.length != train_files_list.length) {
      println("invalid ids and files length:" + train_ids_list.length + "<->" + train_files_list.length)
      return
    }
    for (idx <- train_ids_list.indices) {
      val this_id = train_ids_list(idx)
      val this_file = train_files_list(idx)
      val ctr_file = des_dir + "/" + this_id + "-ctr"
      if (!exists_hdfs_path(ctr_file + "/_SUCCESS")) {
        println("collect ctr of " + this_file)
        delete_hdfs_path(ctr_file)
        val ctr_df: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(this_file)
        ctr_df.rdd.map(
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
      }
    }**/

    /****************************************last_weight***************************************************/
    /**val last_weight_examples = des_dir + "/" + last_date + "-weight-aggr"
    if (!exists_hdfs_path(last_weight_examples + "/_SUCCESS")) {
      delete_hdfs_path(last_weight_examples)
      val df_train_files_last: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_last)

      val weighted_rdd_last = df_train_files_last.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          val bid = dense(10).toString
          val ideal_id = dense(11).toString

          var weight = weight_map.getOrElse(ideal_id + "\t" + bid, 0.0)
          if (weight == 0.0) {
            weight = weight_map_ori.getOrElse(ideal_id, 1.0)
          }
          val weight_reverse = 1.0

          //if (weight <= 1.0f) {
          //  weight = 0.0f
          //}
          //if (label_arr.head != 1L) {
          //  weight = 1.0f
          //}

          Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
        })

      val weighted_rdd_count_last = weighted_rdd_last.count()
      println(s"weighted_rdd_count is : $weighted_rdd_count_last")
      println("DF file count:" + weighted_rdd_count_last.toString + " of file:" + train_files_last)

      val tf_df_last: DataFrame = spark.createDataFrame(weighted_rdd_last, schema_new)
      tf_df_last.repartition(3000).write.format("tfrecords").option("recordType", "Example").save(last_weight_examples)

      //保存count文件
      val fileName_1 = "count_" + Random.nextInt(100000)
      writeNum2File(fileName_1, weighted_rdd_count_last)
      s"hadoop fs -put $fileName_1 $last_weight_examples/count" !

      s"hadoop fs -chmod -R 0777 $last_weight_examples" !
    }**/

    /**
    /****************************************collect_2***************************************************/
    val df_train_files_collect_2: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_2)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_2.printSchema()
    df_train_files_collect_2.show(3)

    val weighted_rdd_2 = df_train_files_collect_2.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        var weight = weight_map.getOrElse(bid, 1.0)
        val weight_reverse = weight_map_reverse.getOrElse(bid, 1.0)

        if (weight <= 1.0f) {
          weight = 0.0f
        }
        //if (label_arr.head != 1L) {
        //  weight = 1.0f
        //}

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

    val weighted_rdd_count_2 = weighted_rdd_2.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_2")
    println("DF file count:" + weighted_rdd_count_2.toString + " of file:" + train_files_collect_2)

    val tf_df_2: DataFrame = spark.createDataFrame(weighted_rdd_2, schema_new)
    tf_df_2.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_2)

    //保存count文件
    val fileName_2 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_2, weighted_rdd_count_2)
    s"hadoop fs -put $fileName_2 $weighted_file_collect_2/count" !

    s"hadoop fs -chmod -R 0777 $weighted_file_collect_2" !

    /****************************************collect_4***************************************************/
    val df_train_files_collect_4: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_4)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_4.printSchema()
    df_train_files_collect_4.show(3)

    val weighted_rdd_4 = df_train_files_collect_4.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        var weight = weight_map.getOrElse(bid, 1.0)
        val weight_reverse = weight_map_reverse.getOrElse(bid, 1.0)

        if (weight <= 1.0f) {
          weight = 0.0f
        }
        //if (label_arr.head != 1L) {
        //  weight = 1.0f
        //}

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

    val weighted_rdd_count_4 = weighted_rdd_4.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_4")
    println("DF file count:" + weighted_rdd_count_4.toString + " of file:" + train_files_collect_4)

    val tf_df_4: DataFrame = spark.createDataFrame(weighted_rdd_4, schema_new)
    tf_df_4.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_4)

    //保存count文件
    val fileName_4 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_4, weighted_rdd_count_4)
    s"hadoop fs -put $fileName_4 $weighted_file_collect_4/count" !

    s"hadoop fs -chmod -R 0777 $weighted_file_collect_4" !

    /****************************************collect_8***************************************************/
    val df_train_files_collect_8: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect_8)
    //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
    df_train_files_collect_8.printSchema()
    df_train_files_collect_8.show(3)

    val weighted_rdd_8 = df_train_files_collect_8.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        var weight = weight_map.getOrElse(bid, 1.0)
        val weight_reverse = weight_map_reverse.getOrElse(bid, 1.0)

        if (weight <= 1.0f) {
          weight = 0.0f
        }
        //if (label_arr.head != 1L) {
        //  weight = 1.0f
        //}

        Row(sample_idx, label_arr, weight.toFloat, weight_reverse.toFloat, dense, idx0, idx1, idx2, idx_arr)
      })

    val weighted_rdd_count_8 = weighted_rdd_8.count()
    println(s"weighted_rdd_count is : $weighted_rdd_count_8")
    println("DF file count:" + weighted_rdd_count_8.toString + " of file:" + train_files_collect_8)

    val tf_df_8: DataFrame = spark.createDataFrame(weighted_rdd_8, schema_new)
    tf_df_8.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect_8)

    //保存count文件
    val fileName_8 = "count_" + Random.nextInt(100000)
    writeNum2File(fileName_8, weighted_rdd_count_8)
    s"hadoop fs -put $fileName_8 $weighted_file_collect_8/count" !

    s"hadoop fs -chmod -R 0777 $weighted_file_collect_8" !**/








    /**
    if (!exists_hdfs_path(bid_cpm_file)) {
      val df_train_files: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files)
      //println("DF file count:" + df_train_files.count().toString + " of file:" + train_files)
      df_train_files.printSchema()
      df_train_files.show(3)
      val info_rdd = df_train_files.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          var label = 0.0
          if (label_arr.head == 1L) {
            label = 1.0
          }

          val bid = dense(10).toString
          //val adclass = dense(16).toString

          val bid_ori = bid_mmh_map.getOrElse(bid, "-1")

          //val output = scala.collection.mutable.ArrayBuffer[String]()
          //output += sample_idx.toString
          //output += label_arr.map(_.toString).mkString(";")
          //output += dense.map(_.toString).mkString(";")
          //output += idx0.map(_.toString).mkString(";")
          //output += idx1.map(_.toString).mkString(";")
          //output += idx2.map(_.toString).mkString(";")
          //output += idx_arr.map(_.toString).mkString(";")

          //(bid, bid_ori, label, 1L, output.mkString("\t"))
          (bid, bid_ori, label, 1.0)
        }
      ).map({
        rs =>
          (rs._1 + "\t" + rs._2, (rs._3, rs._4))
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
        rs =>
          val key_list = rs._1.split("\t")
          val bid_ori = key_list(1).toFloat
          val ctr = rs._2._1/rs._2._2
          val cpm = ctr * bid_ori
          (key_list(0), key_list(1), ctr, cpm, rs._2._1, rs._2._2)
      })

      val total_cpm_map = info_rdd.map({
        rs =>
          ("placeholder", rs._4)
      }).reduceByKey(_ + _).collectAsMap()

      val total_cpm = total_cpm_map.getOrElse("placeholder", 0.0).toFloat
      println("total_cpm=" + total_cpm)

      info_rdd.map({
        rs =>
          val weight = rs._4/total_cpm
          (rs._1, rs._2, rs._3, rs._4, total_cpm, weight, rs._5, rs._6)
      }).repartition(1).sortBy(_._6 * -1).map({
        rs=>
          rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7 + "\t" + rs._8
      }).saveAsTextFile(bid_cpm_file)


      val sta_map = sc.textFile(bid_cpm_file).map({
        rs =>
          val line_list = rs.split("\t")
          val bid_hash = line_list(0)
          val bid_ori = line_list(1)
          val ctr = line_list(2)
          val cpm = line_list(3)
          val total_cpm = line_list(4)
          val weight = line_list(5)
          val click = line_list(6)
          val imp = line_list(7)
          (bid_hash, weight)
      }).collectAsMap()

      println("sta_map.size=" + sta_map.size)

      val schema_new = StructType(List(
        StructField("sample_idx", LongType, nullable = true),
        StructField("label", ArrayType(LongType, containsNull = true)),
        StructField("weight", FloatType, nullable = true),
        StructField("dense", ArrayType(LongType, containsNull = true)),
        StructField("idx0", ArrayType(LongType, containsNull = true)),
        StructField("idx1", ArrayType(LongType, containsNull = true)),
        StructField("idx2", ArrayType(LongType, containsNull = true)),
        StructField("id_arr", ArrayType(LongType, containsNull = true))
      ))


      val df_train_files_collect: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_collect)
      //println("DF file count:" + df_train_files_collect.count().toString + " of file:" + train_files_collect)
      df_train_files_collect.printSchema()
      df_train_files_collect.show(3)

      val weighted_rdd = df_train_files_collect.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          val bid = dense(10).toString
          val weight = sta_map.getOrElse(bid, "1e-5").toFloat

          Row(sample_idx, label_arr, weight, dense, idx0, idx1, idx2, idx_arr)
        })

      val weighted_rdd_count = weighted_rdd.count()
      println(s"weighted_rdd_count is : $weighted_rdd_count")
      println("DF file count:" + weighted_rdd_count.toString + " of file:" + train_files_collect)

      val tf_df: DataFrame = spark.createDataFrame(weighted_rdd, schema_new)
      tf_df.repartition(1200).write.format("tfrecords").option("recordType", "Example").save(weighted_file_collect)

      //保存count文件
      var fileName = "count_" + Random.nextInt(100000)
      writeNum2File(fileName, weighted_rdd_count)
      s"hadoop fs -put $fileName $weighted_file_collect/count" !

      s"hadoop fs -chmod -R 0777 $weighted_file_collect" !


      val df_train_files_sup: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_sup)
      //println("DF file count:" + df_train_files_sup.count().toString + " of file:" + train_files_sup)
      df_train_files_sup.printSchema()
      df_train_files_sup.show(3)

      val sup_weighted_rdd = df_train_files_sup.rdd.map(
        rs => {
          val idx2 = rs.getSeq[Long](0)
          val idx1 = rs.getSeq[Long](1)
          val idx_arr = rs.getSeq[Long](2)
          val idx0 = rs.getSeq[Long](3)
          val sample_idx = rs.getLong(4)
          val label_arr = rs.getSeq[Long](5)
          val dense = rs.getSeq[Long](6)

          val bid = dense(10).toString
          val weight = sta_map.getOrElse(bid, "1e-5").toFloat

          Row(sample_idx, label_arr, weight, dense, idx0, idx1, idx2, idx_arr)
        })

      val sup_weighted_rdd_count = sup_weighted_rdd.count()
      println(s"sup_weighted_rdd_count is : $sup_weighted_rdd_count")
      println("DF file count:" + sup_weighted_rdd_count.toString + " of file:" + train_files_sup)

      val sup_tf_df: DataFrame = spark.createDataFrame(sup_weighted_rdd, schema_new)
      sup_tf_df.repartition(600).write.format("tfrecords").option("recordType", "Example").save(weighted_file_sup)

      //保存count文件
      fileName = "count_" + Random.nextInt(100000)
      writeNum2File(fileName, sup_weighted_rdd_count)
      s"hadoop fs -put $fileName $weighted_file_sup/count" !

      s"hadoop fs -chmod -R 0777 $weighted_file_sup" !
    }**/



    //val total_rdd_count = total_rdd.count()
    //println("total_rdd_count.size=" + total_rdd_count)

    /**
    val positive_rdd = total_rdd.filter(
      rs => {
        val label = rs._3
        var filter = false
        if (label == 1L) {
          filter = true
        }
        filter
      }
    )

    val positive_cnt = positive_rdd.count()
    println("positive_rdd.size=" + positive_cnt)
    val positive_cnt_bc = sc.broadcast(positive_cnt)

    val bid_adclass_info = des_dir + "/bid-adclass-info"
    if (exists_hdfs_path(bid_adclass_info)) {
      delete_hdfs_path(bid_adclass_info)
    }
    val bid_adclass_rdd = positive_rdd.map(
      {
      rs => {
        (rs._1 + "_" + rs._2, rs._3)
      }
      }
    ).reduceByKey(_ + _).map(
      {
        rs =>
          (rs._1, rs._2, positive_cnt_bc.value.toDouble / (positive_cnt_bc.value.toDouble + rs._2.toDouble))
      }
    )

    val component_rdd = bid_adclass_rdd.map(
      {
        rs =>
          (rs._1, rs._3)

      }
    )

    println("bid_adclass_rdd.size=" + bid_adclass_rdd.count())
    bid_adclass_rdd.repartition(1).sortBy(_._2 * -1).map({
      case (key, value1, value2) =>
        key + "\t" + value1.toString + "\t" + value2.toString
    }).saveAsTextFile(bid_adclass_info)


    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("weight", DoubleType, nullable = true),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))


    val df_train_files_latest: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files_latest)
    //println("DF file count:" + importedDfTest.count().toString + " of file:" + test_file)
    df_train_files_latest.printSchema()
    df_train_files_latest.show(3)
    val latest_rdd = df_train_files_latest.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val bid = dense(10).toString
        val adclass = dense(16).toString

        val output = scala.collection.mutable.ArrayBuffer[String]()
        output += sample_idx.toString
        output += label_arr.map(_.toString).mkString(";")
        output += dense.map(_.toString).mkString(";")
        output += idx0.map(_.toString).mkString(";")
        output += idx1.map(_.toString).mkString(";")
        output += idx2.map(_.toString).mkString(";")
        output += idx_arr.map(_.toString).mkString(";")

        (bid + "_" + adclass, output.mkString("\t"))
      }
    )

    val latest_rdd_count = latest_rdd.count()
    println("latest_rdd_count.size=" + latest_rdd_count)

    val weighted_rdd = latest_rdd.join(component_rdd).map({
      rs =>
        val weight = rs._2._2
        val line_list = rs._2._1.split("\t")
        val sample_idx = line_list(0).toLong
        val label_arr = line_list(1).split(";").map(_.toLong).toSeq
        val dense = line_list(2).split(";").map(_.toLong).toSeq
        val idx0 = line_list(3).split(";").map(_.toLong).toSeq
        val idx1 = line_list(4).split(";").map(_.toLong).toSeq
        val idx2 = line_list(5).split(";").map(_.toLong).toSeq
        val idx_arr = line_list(6).split(";").map(_.toLong).toSeq
        Row(sample_idx, label_arr, weight, dense, idx0, idx1, idx2, idx_arr)
    })

    val weighted_rdd_count = latest_rdd_count
    println(s"weighted_rdd_count is : $weighted_rdd_count")

    val weighted_file = des_dir + "/" + curr_date + "-" + time_id + "-weighted"
    val tf_df: DataFrame = spark.createDataFrame(weighted_rdd, schema_new)
    tf_df.repartition(100).write.format("tfrecords").option("recordType", "Example").save(weighted_file)

    //保存count文件
    val fileName = "count_" + Random.nextInt(100000)
    writeNum2File(fileName, weighted_rdd_count)
    s"hadoop fs -put $fileName $weighted_file/count" !

    s"hadoop fs -chmod -R 0777 $weighted_file" !
      **/


    /**
    val bid_info = des_dir + "/bid-info"
    if (exists_hdfs_path(bid_info)) {
      delete_hdfs_path(bid_info)
    }
    val bid_rdd = positive_rdd.map({
      rs => {
        (rs._1, rs._3)
      }
    }).reduceByKey(_ + _)
    .map({
      rs =>
        (rs._1, rs._2, total_count.value.toDouble / (total_count.value.toDouble + rs._2.toDouble))
    })
    println("bid_rdd.size=" + bid_rdd.count())
    bid_rdd.repartition(1).sortBy(_._2 * -1).map({
      case (key, value1, value2) =>
        key + "\t" + value1.toString + "\t" + value2
    }).saveAsTextFile(bid_info)


    val adclass_info = des_dir + "/adclass-info"
    if (exists_hdfs_path(adclass_info)) {
      delete_hdfs_path(adclass_info)
    }
    val adclass_rdd = positive_rdd.map({
      rs => {
        (rs._2, rs._3)
      }
    }).reduceByKey(_ + _)
    println("adclass_rdd.size=" + adclass_rdd.count())
    adclass_rdd.repartition(1).sortBy(_._2 * -1).map({
      case (key, value) =>
        key + "\t" + value.toString
    }).saveAsTextFile(adclass_info)


    val bid_adclass_info = des_dir + "/bid-adclass-info"
    if (exists_hdfs_path(bid_adclass_info)) {
      delete_hdfs_path(bid_adclass_info)
    }
    val bid_adclass_rdd = positive_rdd.map({
      rs => {
        (rs._1 + "_" + rs._2, rs._3)
      }
    }).reduceByKey(_ + _)
    println("bid_adclass_rdd.size=" + bid_adclass_rdd.count())
    bid_adclass_rdd.repartition(1).sortBy(_._2 * -1).map({
      case (key, value) =>
        key + "\t" + value.toString
    }).saveAsTextFile(bid_adclass_info)**/



    /**
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("Extract Test Examples' AD Info")
    val text_test = des_dir + "/" + curr_date + "-" + time_id + "-test"

    if (exists_hdfs_path(text_test)) {
      delete_hdfs_path(text_test)
    }

    val importedDfTest: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(test_file)
    println("DF file count:" + importedDfTest.count().toString + " of file:" + test_file)
    importedDfTest.printSchema()
    importedDfTest.show(3)
    val totalMap = importedDfTest.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        //11,12,13,14
        //,ideaid,unitid,planid,userid
        //(dense(11).toString, dense(12).toString, dense(13).toString, dense(14).toString)
        val output = scala.collection.mutable.ArrayBuffer[String]()
        output += dense(11).toString
        output += dense(12).toString
        output += dense(13).toString
        output += dense(14).toString
        output.mkString("\t")
      }
    ).flatMap(
      rs => {
        val line = rs.split("\t")
        for (elem <- line)
          yield (elem, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("totalMap.size=" + totalMap.size)

    //.repartition(100).saveAsTextFile(text_test)
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println("Filter Train Examples")
    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))

    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(train_files)
    println("DF file count:" + importedDf.count().toString + " of train files")
    val text_train_rdd = importedDf.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val idealId = dense(11).toString
        val unitId = dense(12).toString
        val planId = dense(13).toString
        val userId = dense(14).toString

        val output = scala.collection.mutable.ArrayBuffer[String]()
        output += sample_idx.toString
        output += label_arr.map(_.toString).mkString(";")
        output += dense.map(_.toString).mkString(";")
        output += idx0.map(_.toString).mkString(";")
        output += idx1.map(_.toString).mkString(";")
        output += idx2.map(_.toString).mkString(";")
        output += idx_arr.map(_.toString).mkString(";")
        (idealId, unitId, planId, userId, output.mkString("\t"))
      }
    )

    val ideal_id_filter_rdd = text_train_rdd
      .filter(
      rs => {
        val idealId = rs._1
        var filter = true
        if (!totalMap.contains(idealId)) {
          filter = false
        }
        filter
      }
    ).map(
      rs => {
        rs._5
      }
    )

    val unit_id_filter_rdd = text_train_rdd
      .filter(
        rs => {
          val unitId = rs._2
          var filter = true
          if (!totalMap.contains(unitId)) {
            filter = false
          }
          filter
        }
      ).map(
      rs => {
        rs._5
      }
    )

    val plan_id_filter_rdd = text_train_rdd
      .filter(
        rs => {
          val planId = rs._3
          var filter = true
          if (!totalMap.contains(planId)) {
            filter = false
          }
          filter
        }
      ).map(
      rs => {
        rs._5
      }
    )

    val user_id_filter_rdd = text_train_rdd
      .filter(
        rs => {
          val userId = rs._4
          var filter = true
          if (!totalMap.contains(userId)) {
            filter = false
          }
          filter
        }
      ).map(
      rs => {
        rs._5
      }
    )

    val ideal_id_filter_rdd_count = ideal_id_filter_rdd.count
    println(s"ideal_id_filter_rdd_count is : $ideal_id_filter_rdd_count")

    val unit_id_filter_rdd_count = unit_id_filter_rdd.count
    println(s"unit_id_filter_rdd_count is : $unit_id_filter_rdd_count")

    val plan_id_filter_rdd_count = plan_id_filter_rdd.count
    println(s"plan_id_filter_rdd_count is : $plan_id_filter_rdd_count")

    val user_id_filter_rdd_count = user_id_filter_rdd.count
    println(s"user_id_filter_rdd_count is : $user_id_filter_rdd_count")
    **/

      /**
    val importedDfHistory: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(history_files)
    println("history DF file count:" + importedDfHistory.count().toString + " of train files")
    importedDfHistory.printSchema()
    importedDfHistory.show(3)
    val his_rdd = importedDfHistory.rdd.map(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4)
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        val idealId = dense(11).toString
        val unitId = dense(12).toString
        val planId = dense(13).toString
        val userId = dense(14).toString
        val adclass = dense(16).toString

        (idealId, unitId, planId, userId, adclass)
      }
    )

    val ideal_id_single = his_rdd.map(
      rs => {
          (rs._1, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("ideal_id_single.size=" + ideal_id_single.size)
    val ideal_id_adclass = his_rdd.map(
      rs => {
        (rs._1 + "_" + rs._5, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("ideal_id_adclass.size=" + ideal_id_adclass.size)

    val unit_id_single = his_rdd.map(
      rs => {
        (rs._2, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("unit_id_single.size=" + unit_id_single.size)
    val unit_id_adclass = his_rdd.map(
      rs => {
        (rs._2 + "_" + rs._5, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("unit_id_adclass.size=" + unit_id_adclass.size)

    val plan_id_single = his_rdd.map(
      rs => {
        (rs._3, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("plan_id_single.size=" + plan_id_single.size)
    val plan_id_adclass = his_rdd.map(
      rs => {
        (rs._3 + "_" + rs._5, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("plan_id_adclass.size=" + plan_id_adclass.size)

    val user_id_single = his_rdd.map(
      rs => {
        (rs._4, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("user_id_single.size=" + user_id_single.size)
    val user_id_adclass = his_rdd.map(
      rs => {
        (rs._4 + "_" + rs._5, 1L)
      }
    ).reduceByKey(_ + _).collectAsMap()
    println("user_id_adclass.size=" + user_id_adclass.size)
        **/






    //val tf_train_rdd = text_train_rdd.map({
    //  rs =>
    //    val rs_list = rs.split("\t")
    //    val sample_idx = rs_list(0).toLong
    //    val label_arr = rs_list(1).split(";").map(_.toLong).toSeq
    //    val dense = rs_list(2).split(";").map(_.toLong).toSeq
    //    val idx0 = rs_list(3).split(";").map(_.toLong).toSeq
    //    val idx1 = rs_list(4).split(";").map(_.toLong).toSeq
    //    val idx2 = rs_list(5).split(";").map(_.toLong).toSeq
    //    val idx_arr = rs_list(6).split(";").map(_.toLong).toSeq
    //    Row(sample_idx, label_arr, dense, idx0, idx1, idx2, idx_arr)
    //})

    ////val tf_train_rdd_count = tf_train_rdd.count
    ////println(s"tf_train_rdd_count is : $tf_train_rdd_count")

    //val text_train = des_dir + "/" + curr_date + "-" + time_id + "-train-text"
    //val tf_train = des_dir + "/" + curr_date + "-" + time_id + "-train-tf"

    //if (exists_hdfs_path(text_train)) {
    //  delete_hdfs_path(text_train)
    //}

    //if (exists_hdfs_path(tf_train)) {
    //  delete_hdfs_path(tf_train)
    //}

    //val tf_df: DataFrame = spark.createDataFrame(tf_train_rdd, schema_new)
    //tf_df.repartition(100).write.format("tfrecords").option("recordType", "Example").save(tf_train)

    ////保存count文件
    //val fileName = "count_" + Random.nextInt(100000)
    //writeNum2File(fileName, text_train_rdd_count)
    //s"hadoop fs -put $fileName $tf_train/count" !

    //s"hadoop fs -chmod -R 0777 $tf_train" !

    //text_train_rdd.repartition(100).saveAsTextFile(text_train)
    //s"hadoop fs -put $fileName $text_train/count" !

    //s"hadoop fs -chmod -R 0777 $text_train" !

  }
}
