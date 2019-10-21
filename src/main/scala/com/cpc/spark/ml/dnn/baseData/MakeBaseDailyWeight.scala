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


    /** println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      **
      *val bid_cpm_file = des_dir + "/" + curr_date + "-21days-weight-info"
      **
      *if (!exists_hdfs_path(bid_cpm_file)) {
      * val df_train_files: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(file_list)
      * //println("DF file count:" + df_train_files.count().toString + " of file:" + train_files)
      * df_train_files.printSchema()
      * df_train_files.show(3)
      * val info_rdd = df_train_files.rdd.map(
      * rs => {
      * val idx2 = rs.getSeq[Long](0)
      * val idx1 = rs.getSeq[Long](1)
      * val idx_arr = rs.getSeq[Long](2)
      * val idx0 = rs.getSeq[Long](3)
      * val sample_idx = rs.getLong(4)
      * val label_arr = rs.getSeq[Long](5)
      * val dense = rs.getSeq[Long](6)
      **
      *var label = 0.0
      * if (label_arr.head == 1L) {
      * label = 1.0
      * }
      **
      *val bid = dense(10).toString
      * val bid_ori = bid_mmh_map.getOrElse(bid, "-1")
      * (bid, bid_ori, label, 1.0)
      * }
      * ).map({
      * rs =>
      * (rs._1 + "\t" + rs._2, (rs._3, rs._4))
      * }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
      * rs =>
      * val key_list = rs._1.split("\t")
      * val bid_ori = key_list(1).toFloat
      * val ctr = rs._2._1 / rs._2._2
      * val cpm = ctr * bid_ori
      * (key_list(0), key_list(1), ctr, cpm, rs._2._1, rs._2._2)
      * })
      **
      *val total_cpm_map = info_rdd.map({
      * rs =>
      * ("placeholder", rs._4)
      * }).reduceByKey(_ + _).collectAsMap()
      **
      *val total_cpm = total_cpm_map.getOrElse("placeholder", 0.0).toFloat
      * println("total_cpm=" + total_cpm)
      **
    *info_rdd.map({
      * rs =>
      * val weight = rs._4 / total_cpm
      * (rs._1, rs._2, rs._3, rs._4, total_cpm, weight, rs._5, rs._6)
      * }).repartition(1).sortBy(_._6 * -1).map({
      * rs =>
      *rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7 + "\t" + rs._8
      * }).saveAsTextFile(bid_cpm_file)
      * } **/

    val train_date_list = date_list.split(";")
    val train_file_list = train_list.split(";")

    for (idx <- train_date_list.indices) {
      val this_date = train_date_list(idx)
      val this_file = train_file_list(idx)
      val bid_cpm_file_curr = des_dir + "/" + this_date + "-samples-info"
      if (!exists_hdfs_path(bid_cpm_file_curr + "/_SUCCESS")) {
        println("collect samples info of " + this_file)
        delete_hdfs_path(bid_cpm_file_curr)
        val df_train_files: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(this_file)
        //println("DF file count:" + df_train_files.count().toString + " of file:" + train_files)
        df_train_files.printSchema()
        df_train_files.show(3)
        df_train_files.rdd.map(
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

            val ideal_id = dense(11).toString
            val bid = dense(10).toString
            val bid_ori = bid_mmh_map.getOrElse(bid, "-1")
            (ideal_id + "\t" + bid + "\t" + bid_ori, (label, 1.0))
          }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
          rs =>
            rs._1 + "\t" + rs._2._1 + "\t" + rs._2._2
        }).repartition(1).saveAsTextFile(bid_cpm_file_curr)
      }
    }

    val cpm_file_buffer = scala.collection.mutable.ArrayBuffer[String]()
    for (idx <- train_date_list.indices) {
      val this_date = train_date_list(idx)
      val bid_cpm_file_curr = des_dir + "/" + this_date + "-samples-info"
      if (exists_hdfs_path(bid_cpm_file_curr)) {
        cpm_file_buffer += bid_cpm_file_curr
      }
    }

    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    val bid_cpm_file = des_dir + "/" + curr_date + "-14days-weight-info"

    if (!exists_hdfs_path(bid_cpm_file)) {

      val info_rdd = sc.textFile(cpm_file_buffer.mkString(",")).map(
        rs => {
          val rs_list = rs.split("\t")
          (rs_list(0) + "\t" + rs_list(1) + "\t" + rs_list(2), (rs_list(3).toDouble, rs_list(4).toDouble))
        }
      ).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
        rs =>
          val key_list = rs._1.split("\t")
          val ideal_id = key_list(0)
          val bid_hash = key_list(1)
          val bid_ori = key_list(2)
          val ctr = rs._2._1 / rs._2._2
          val cpm = ctr * bid_ori.toFloat
          (ideal_id, bid_hash, bid_ori, ctr, cpm, rs._2._1, rs._2._2)
      })

      val total_clk_map = info_rdd.map({
        rs =>
          ("placeholder", rs._6.toDouble)
      }).reduceByKey(_ + _).collectAsMap()
      val total_clk = total_clk_map.getOrElse("placeholder", 1.0).toFloat
      println("total_clk=" + total_clk)

      val total_ideal_id_map = info_rdd.map({
        rs =>
          (rs._1, 1)
      }).reduceByKey(_ + _).collectAsMap()
      val total_ideal_id = total_ideal_id_map.size
      println("total_deal_id=" + total_ideal_id)

      val total_cpm_map = info_rdd.map({
        rs =>
          ("placeholder", rs._5)
      }).reduceByKey(_ + _).collectAsMap()

      val total_cpm = total_cpm_map.getOrElse("placeholder", 0.0).toFloat
      println("total_cpm=" + total_cpm)

      info_rdd.map({
        rs =>
          val weight = rs._5 / total_cpm
          val clk_percentage = rs._6.toDouble / total_clk
          (rs._1, rs._2, rs._3, rs._4, rs._5, total_cpm, weight, rs._6, rs._7, clk_percentage)
      }).repartition(1).sortBy(_._8 * -1).map({
        rs =>
          rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7 + "\t" + rs._8 + "\t" + rs._9 + "\t" + rs._10
      }).saveAsTextFile(bid_cpm_file)
    }


    val last_file = train_file_list(0)
    val last_date = train_date_list(0)

    println("curr_date:" + curr_date)
    println("last_file:" + last_file)
    println("last_date:" + last_date)


    val base_daily_bid_cpm_file = des_dir + s"/$curr_date-14days-weight-info"
    println("base_daily_bid_cpm_file=" + base_daily_bid_cpm_file)

    if (!exists_hdfs_path(base_daily_bid_cpm_file)) {
      println("no base daily bid cpm file, exiting...")
      return
    }

    val minus_clk_cnt = 60000.0

    val sta_rdd = sc.textFile(base_daily_bid_cpm_file).map({
      rs =>
        val line_list = rs.split("\t")
        val ideal_id = line_list(0)
        val bid_hash = line_list(1)
        val bid_ori = line_list(2)
        val ctr = line_list(3)
        val cpm = line_list(4)
        val total_cpm = line_list(5)
        val weight = line_list(6)
        val click = line_list(7)
        val imp = line_list(8)
        (ideal_id, bid_hash, bid_ori, weight, click, imp, ctr)
    })
    println("sta_rdd.size=" + sta_rdd.count())

    val bid_weight_map = sta_rdd.map({
      rs =>
        ("max", (rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._7 + "\t" + rs._4, rs._5.toDouble))
    }).reduceByKey((x, y) => if (x._2 >= y._2) (x._1, x._2) else (y._1, y._2)).map({
      rs =>
        (rs._1, rs._2._1 + "\t" + rs._2._2)
    }).collectAsMap()

    val max_info = bid_weight_map.getOrElse("max", "0\t0\t0\t0\t0\t0")
    println("max_info_list=" + max_info)
    val max_info_list = max_info.split("\t")

    val max_clk = max_info_list(5).toDouble
    val max_weight = max_info_list(4).toDouble
    val max_ctr = max_info_list(3).toDouble

    if (max_weight <= 0.0 || max_ctr <= 0.0) {
      println("invalid max_weight:" + max_weight)
      println("invalid max_ctr:" + max_ctr)
      return
    }

    //(ideal_id, bid_hash, bid_ori, weight, click, imp, ctr)
    val max_map_first = sta_rdd.map({
      rs =>
        val weight = rs._4.toDouble
        var weight_new = 1.0
        val click = rs._5.toDouble
        val ctr = rs._7.toDouble
        if (click >= minus_clk_cnt && weight >= max_weight) {
          weight_new = weight / max_weight
        } else {
          weight_new = 1.0
        }
        ("max_weight_placeholder", weight_new)
    }).reduceByKey((x, y) => math.max(x, y)).collectAsMap()

    val max_weight_first = max_map_first.getOrElse("max_weight_placeholder", 1.0)
    println("max_weight_first:" + max_weight_first)
    val max_weight_factor_first = 9.0
    val factor_first = max_weight_factor_first / (max_weight_first.toDouble - 1.0)
    println("factor_first:" + factor_first)

    //(ideal_id, bid_hash, bid_ori, weight, click, imp, ctr)
    val weight_map_rdd = sta_rdd.map({
      rs =>
        val ideal_id = rs._1
        val bid_hash = rs._2
        val bid_ori = rs._3
        val weight = rs._4.toDouble
        var weight_new_ori = 1.0
        var weight_new_norm = 1.0
        val click = rs._5.toDouble
        val ctr = rs._7.toDouble
        if (click >= minus_clk_cnt && weight >= max_weight) {
          weight_new_ori = weight / max_weight
          weight_new_norm = 1.0 + (weight / max_weight - 1.0) * factor_first
        }
        //weight_new = 1.0 / weight_new
        (ideal_id, bid_hash, weight_new_ori)
    })

    val weight_map = weight_map_rdd.map({
      rs =>
        (rs._1 + "\t" + rs._2, rs._3)
    }).collectAsMap()
    val weight_map_ori = weight_map_rdd.map({
      rs =>
        (rs._1, rs._3)
    }).collectAsMap()

    //(ideal_id, bid_hash, bid_ori, weight, click, imp, ctr)
    val weight_map_file = des_dir + "/" + curr_date + "-weight-map"
    if (!exists_hdfs_path(weight_map_file + "_SUCCESS")) {
      delete_hdfs_path(weight_map_file)
      sta_rdd.map({
        rs =>
          val ideal_id = rs._1
          val bid_hash = rs._2
          val bid_ori = rs._3
          val weight = rs._4.toDouble
          var weight_new_ori = 1.0
          var weight_new_norm = 1.0
          val click = rs._5.toDouble
          val imp = rs._6
          val ctr = rs._7.toDouble

          if (click >= minus_clk_cnt && weight >= max_weight) {
            weight_new_ori = weight / max_weight
            weight_new_norm = 1.0 + (weight / max_weight - 1.0) * factor_first
            if (weight == max_weight) {
              weight_new_ori = 1.0000001
              weight_new_norm = 1.0000001
            }
          }
          (ideal_id, bid_hash, bid_ori, weight_new_norm, weight_new_ori, weight, ctr, click, imp)
      }).repartition(1).sortBy(_._4 * -1).map({
        rs =>
          rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5 + "\t" + rs._6 + "\t" + rs._7 + "\t" + rs._8 + "\t" + rs._9
      }).saveAsTextFile(weight_map_file)
    }

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

    val last_weight_examples = des_dir + "/" + last_date + "-weight-aggr"
    if (!exists_hdfs_path(last_weight_examples + "/_SUCCESS")) {
      delete_hdfs_path(last_weight_examples)
      val df_train_files_last: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(last_file)

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
      println("DF file count:" + weighted_rdd_count_last.toString + " of file:" + last_file)

      val tf_df_last: DataFrame = spark.createDataFrame(weighted_rdd_last, schema_new)
      tf_df_last.repartition(3000).write.format("tfrecords").option("recordType", "Example").save(last_weight_examples)

      //保存count文件
      val fileName_1 = "count_" + Random.nextInt(100000)
      writeNum2File(fileName_1, weighted_rdd_count_last)
      s"hadoop fs -put $fileName_1 $last_weight_examples/count" !

      s"hadoop fs -chmod -R 0777 $last_weight_examples" !
    }

    val hour_mmh_map_file = des_dir + "/" + "hour_mmh_map.txt"
    val hour_mmh_map = sc.textFile(hour_mmh_map_file).map({
      rs =>
        val line_list = rs.split("\t")
        (line_list(1), line_list(0))
    }).collectAsMap()
    val hour_mmh_map_reverse = sc.textFile(hour_mmh_map_file).map({
      rs =>
        val line_list = rs.split("\t")
        (line_list(0), line_list(1))
    }).collectAsMap()
    println("hour_mmh_map.size=" + hour_mmh_map.size)


    val df_train_files_last: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(last_file)
    val hour_str = "00;01;02;03;04;05;06;07;08;09;10;11;12;13;14;15;16;17;18;19;20;21;22;23"
    val hour_list = hour_str.split(";")

    for (idx <- hour_list.indices) {
      val this_hour = hour_list(idx)
      val this_hour_hash = hour_mmh_map.getOrElse(this_hour, "-1")
      val hour_example_file = des_dir + "/" + last_date + "-" + this_hour
      val hour_ctr_file = des_dir + "/" + last_date + "-" + this_hour + "-ctr"
      if (!exists_hdfs_path(hour_example_file + "/_SUCCESS") || !exists_hdfs_path(hour_ctr_file + "/_SUCCESS")) {
        delete_hdfs_path(hour_example_file)
        delete_hdfs_path(hour_ctr_file)

        df_train_files_last.rdd.map(
          rs => {
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            val hour = dense(27).toString
            val hour_ori = hour_mmh_map_reverse.getOrElse(hour, "-1")

            var label = 0.0
            if (label_arr.head == 1L) {
              label = 1.0
            }
            (hour + "\t" + hour_ori, (label, 1.0))
          }
        ).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map({
          rs =>
            val line_list = rs._1.split("\t")
            (line_list(0), line_list(1), rs._2._1, rs._2._2, rs._2._1 / rs._2._2)
        }).filter({
          rs =>
            if (rs._2 == this_hour) {
              true
            } else {
              false
            }
        }).repartition(1).map({
          rs =>
            rs._1 + "\t" + rs._2 + "\t" + rs._3 + "\t" + rs._4 + "\t" + rs._5

        }).saveAsTextFile(hour_ctr_file)


        val weighted_rdd_hour = df_train_files_last.rdd.filter({
          rs =>
            val dense = rs.getSeq[Long](6)
            val hour = dense(27).toString
            if (hour == this_hour_hash) {
              true
            } else {
              false
            }
        }).map(
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

        val weighted_rdd_count_hour = weighted_rdd_hour.count()
        println(s"weighted_rdd_count is : $weighted_rdd_count_hour")
        println("DF file count:" + weighted_rdd_count_hour.toString + " of file:" + hour_example_file)

        val tf_df_last: DataFrame = spark.createDataFrame(weighted_rdd_hour, schema_new)
        tf_df_last.repartition(3000).write.format("tfrecords").option("recordType", "Example").save(hour_example_file)

        //保存count文件
        val fileName_1 = "count_" + Random.nextInt(100000)
        writeNum2File(fileName_1, weighted_rdd_count_hour)
        s"hadoop fs -put $fileName_1 $hour_example_file/count" !

        s"hadoop fs -chmod -R 0777 $hour_example_file" !









      }
    }



  }
}

