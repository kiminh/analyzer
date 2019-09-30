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
  * @author fenghuabin
  * @version 1.0
  *
  */

object MakeBaseDailyWeight {


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
    if (args.length != 6) {
      System.err.println(
        """
          |you have to input 6 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(des_dir, file_list, curr_date, delete_old, train_list, date_list) = args

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

    val bid_cpm_file = des_dir + "/" + curr_date + "-21days-weight-info"

    if (!exists_hdfs_path(bid_cpm_file)) {
      val df_train_files: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(file_list)
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
          val bid_ori = bid_mmh_map.getOrElse(bid, "-1")
          (bid, bid_ori, label, 1.0)
        }
      ).map({
        rs =>
          (rs._1 + "\t" + rs._2, (rs._3, rs._4))
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
        rs =>
          val key_list = rs._1.split("\t")
          val bid_ori = key_list(1).toFloat
          val ctr = rs._2._1 / rs._2._2
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
          val weight = rs._4 / total_cpm
          (rs._1, rs._2, rs._3, rs._4, total_cpm, weight, rs._5, rs._6)
      }).repartition(1).sortBy(_._6 * -1).map({
        rs =>
          rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7 + "\t" + rs._8
      }).saveAsTextFile(bid_cpm_file)
    }

    return

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

    val train_date_list = date_list.split(";")
    val train_file_list = train_list.split(";")

    for (idx <- train_date_list.indices) {
      val this_date = train_date_list(idx)
      val this_file = train_file_list(idx)
      val bid_cpm_file = des_dir + "/" + this_date + "-21days-weight-info"
      if (exists_hdfs_path(bid_cpm_file)) {
        val sta_rdd = sc.textFile(bid_cpm_file).map({
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
            (bid_hash, bid_ori, weight, click, imp, ctr)
        })
        println("sta_rdd.size=" + sta_rdd.count())

        val bid_ori_map = sta_rdd.map({
          rs =>
            (rs._2, rs._3.toFloat)
        }).collectAsMap()

        val bid_1_weight = bid_ori_map.getOrElse("1", 0.0f)
        println("bid_1_weight:" + bid_1_weight)
        if (bid_1_weight <= 0.0) {
          println("invalid bid_1_weight:" + bid_1_weight)
          return
        }

        val max_map = sta_rdd.map({
          rs =>
            val bid_hash = rs._1
            val weight = rs._3.toFloat
            var weight_new = 1.0
            val click = rs._4.toFloat
            if (click >= 1000000.0 && weight > bid_1_weight) {
              weight_new = weight / bid_1_weight
            }
            if (weight <= 0.0) {
              weight_new = 1.0
            }
            ("max_weight_placeholder", weight_new)
        }).reduceByKey((x, y) => math.max(x, y)).collectAsMap()

        val max_weight = max_map.getOrElse("max_weight_placeholder", 1.0)
        println("max_weight:" + max_weight)
        val max_weight_factor = 0.2f
        val factor = max_weight_factor / (max_weight.toFloat - 1.0)
        println("factor:" + factor)

        val weight_map = sta_rdd.map({
          rs =>
            val bid_hash = rs._1
            val weight = rs._3.toFloat
            var weight_new = 1.0
            val click = rs._4.toFloat
            if (click >= 1000000.0 && weight > bid_1_weight) {
              weight_new = 1.0 + (weight / bid_1_weight - 1.0) * factor
            }
            if (weight <= 0.0) {
              weight_new = 1.0
            }
            //weight_new = 1.0 / weight_new
            (bid_hash, weight_new)
        }).collectAsMap()
        val weight_map_file = des_dir + "/" + this_date + "-21days-weight-map"
        if (exists_hdfs_path(weight_map_file)) {
          delete_hdfs_path(weight_map_file)
        }
        sta_rdd.map({
          rs =>
            val bid_hash = rs._1
            val bid_ori = rs._2
            val weight = rs._3.toFloat
            var weight_new = 1.0
            val click = rs._4.toFloat
            if (click >= 1000000.0 && weight > bid_1_weight) {
              weight_new = 1.0 + (weight / bid_1_weight - 1.0) * factor
            }
            if (weight <= 0.0) {
              weight_new = 1.0
            }
            //weight_new = 1.0 / weight_new
            (bid_hash, bid_ori, weight_new.toFloat, weight.toFloat, click, rs._5, rs._6)
          //bid_hash + "\t" + weight_new
        }).repartition(1).sortBy(_._3 * -1).map({
          rs =>
            rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7
        }).saveAsTextFile(weight_map_file)



        val weighted_train_file = des_dir + "/" + this_date + "-w"
        if (delete_old == "true" && exists_hdfs_path(weighted_train_file)) {
          delete_hdfs_path(weighted_train_file)
        }

        val df_train: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(this_file)

        val weighted_rdd = df_train.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            val bid = dense(10).toString
            val weight = weight_map.getOrElse(bid, 1.0).toFloat

            Row(sample_idx, label_arr, weight, dense, idx0, idx1, idx2, idx_arr)
          })

        val weighted_rdd_count = weighted_rdd.count()
        println(s"weighted_rdd_count is : $weighted_rdd_count")
        println("DF file count:" + weighted_rdd_count.toString + " of file:" + this_file)

        val tf_df: DataFrame = spark.createDataFrame(weighted_rdd, schema_new)
        tf_df.repartition(1200).write.format("tfrecords").option("recordType", "Example").save(weighted_train_file)

        //保存count文件
        var fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, weighted_rdd_count)
        s"hadoop fs -put $fileName $weighted_train_file/count" !

        s"hadoop fs -chmod -R 0777 $weighted_train_file" !
      }
    }
  }
}

