package com.cpc.spark.ml.dnn.baseData

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.process._
import scala.util.Random
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ArrayBuffer

/**
  * 解析tfrecord到hdfs并统计区间sparse feature出现的值和做映射以及负采样
  * created time : 2019/07/13 10:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object MakeTrainExamples {



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

  //def getColAtIndex(id:Int): Column = {
  //  col(s"column1")(id).as(s"column1_${id+1}")
  //}


  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
          |you have to input 5 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    //val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args
    val Array(src_dir, src_date_str, des_dir, instances_file, numPartitions) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val src_date_list = src_date_str.split(";")

    /************make text examples************************/
    println("Make text examples")
    for (src_date <- src_date_list) {
      val curr_file_src = src_dir + "/" + src_date
      val tf_text = des_dir + "/" + src_date + "-text"
      if (!exists_hdfs_path(tf_text) && exists_hdfs_path(curr_file_src)) {
        val curr_file_src_collect = src_dir + "/" + src_date + "/part-r-*"
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src_collect)
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src_collect)
        importedDf.printSchema()
        importedDf.show(3)

        importedDf.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            var label = "0.0"
            if (label_arr.head == 1L) {
              label = "1.0"
            }

            val output = scala.collection.mutable.ArrayBuffer[String]()
            output += sample_idx.toString
            output += label
            output += label_arr.map(_.toString).mkString(";")
            output += dense.map(_.toString).mkString(";")
            output += idx0.map(_.toString).mkString(";")
            output += idx1.map(_.toString).mkString(";")
            output += idx2.map(_.toString).mkString(";")
            output += idx_arr.map(_.toString).mkString(";")

            output.mkString("\t")
          }
        ).saveAsTextFile(tf_text)
      }
    }
    println("Done.......")

    /************Collect instances for sparse features************************/
    println("Collect sparse features' values and map to continuous index")

    val instances_all = des_dir + "/" + instances_file
    if (!exists_hdfs_path(instances_all)) {
      var data = sc.parallelize(Array[(String, Long)]())
      for (src_date <- src_date_list) {
        val tf_text = des_dir + "/" + src_date + "-text"
        if (exists_hdfs_path(tf_text)) {
          data = data.union(
            sc.textFile(tf_text).map(
              rs => {
                val line_list = rs.split("\t")
                val dense = line_list(3).split(";")
                val idx_arr = line_list(7).split(";")
                val output = ArrayBuffer[String]()
                for (idx <- dense.indices) {
                  output += dense(idx)
                }
                for (idx <- idx_arr.indices) {
                  output += idx_arr(idx)
                }
                output.mkString("\t")
              }
            ).flatMap(
              rs => {
                val line = rs.split("\t")
                for (elem <- line)
                  yield (elem, 1L)
              }
            ).reduceByKey(_ + _)
          ).reduceByKey(_ + _)
        }
      }

      data.reduceByKey(_ + _).repartition(1).sortBy(_._2 * -1).map {
        case (key, value) =>
          key + "\t" + value.toString
      }.saveAsTextFile(instances_all)
    }

    val instances_all_map = des_dir + "/" + instances_file + "-mapped"
    if (!exists_hdfs_path(instances_all_map) && exists_hdfs_path(instances_all)) {
      val acc = new LongAccumulator
      spark.sparkContext.register(acc)
      sc.textFile(instances_all).coalesce(1, false).map{
        rs => {
          acc.add(1L)
          val line = rs.split("\t")
          val key = line(0)
          (key, acc.count)
        }
      }.repartition(1).sortBy(_._2).map{
        case (key, value) => key + "\t" + value.toString
      }.saveAsTextFile(instances_all_map)
    }
    println("Done.......")


    /************************load map********************************/
    println("Load Uid SparseMap")
    val instances_all_map_uid = des_dir + "/" + instances_file + "-mapped-uid"
    val sparseMapUid = sc.textFile(instances_all_map_uid).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0).toLong
        val key = (line(1).toLong - 1L).toString
        (field, key)
      }
    }
    println("sparseMapUid.size=" + sparseMapUid.count)

    println("Load Others SparseMap")
    val instances_all_map_others = des_dir + "/" + instances_file + "-mapped-others"
    val sparseMapOthers = sc.textFile(instances_all_map_others).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0).toLong
        val key = (line(1).toLong - 1L).toString
        (field, key)
      }
    }
    println("sparseMapOthers.size=" + sparseMapOthers.count)
    val sparse_map_others_count = sparseMapOthers.count

    /************check sid************************/
    //println("Check Sample Index")
    //for (src_date <- src_date_list) {
    //  val tf_text = des_dir + "/" + src_date + "-text"
    //  if (exists_hdfs_path(tf_text)) {
    //    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //    println("date:" + src_date)
    //    val rdd = sc.textFile(tf_text).map(
    //      f = rs => {
    //        val line_list = rs.split("\t")
    //        (line_list(0), 1)
    //      }
    //    )
    //    println("text lines:" + rdd.count.toString)
    //    val reduce_lines = rdd.reduceByKey(_ + _).count
    //    println("reduced lines:" + reduce_lines)
    //  }
    //}
    //println("Done.......")

    println("Do Mapping Complete Features")
    for (src_date <- src_date_list) {
      val tf_text_mapped_others = des_dir + "/" + src_date + "-text-mapped-others"
      val tf_text = des_dir + "/" + src_date + "-text"
      if (!exists_hdfs_path(tf_text_mapped_others) && exists_hdfs_path(tf_text)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("make " + tf_text_mapped_others)
        val value_rdd = sc.textFile(tf_text).map(
          rs => {
            val line_list = rs.split("\t")
            val sid = line_list(0)
            val dense = line_list(3).split(";")
            val idx_arr = line_list(7).split(";")
            val keys_other = scala.collection.mutable.ArrayBuffer[String]()
            keys_other += sid
            for (idx <- dense.indices) {
              if (idx != 25) {
                keys_other += dense(idx)
              }
            }
            for (idx <- idx_arr.indices) {
                keys_other += idx_arr(idx)
            }
            keys_other.mkString("\t")
          }
        ).flatMap(
          rs => {
            val line_list = rs.split("\t")
            val sid = line_list(0)
            for (idx <- 1 until line_list.length)
              yield (line_list(idx).toLong, Array[(String, Int)]((sid, idx - 1)))
          }
        ).reduceByKey(_ ++ _)

        //RDD[(Long, (Array[(String, Int)], String))]
        val value_rdd_join = value_rdd.join(sparseMapOthers)

        val value_rdd_join_reduced = value_rdd_join.flatMap(
          rs => {
            val pairs_array = rs._2._1
            val mapped_id = rs._2._2
            for ((sid, idx) <- pairs_array)
              yield (sid, Array[(Int, String)](idx, mapped_id))
          }
        ).reduceByKey(_ ++ _)

        //val value_rdd_join_reduced = value_rdd_join.map({
        //  case(_, ((sid, idx), mapped_id)) =>
        //    (sid, Array((idx, mapped_id)))
        //}).reduceByKey(_ ++ _)


        //println("value_rdd_count:" + value_rdd.count)
        //println("value_rdd_join_count:" + value_rdd_join.count)
        //println("value_rdd_join_reduced_count:" + value_rdd_join_reduced.count)

        val value_rdd_join_reduced_compact = value_rdd_join_reduced.map({
          case(sid, mapped_pair_array) =>
            val total_len = mapped_pair_array.length
            val len_one_hot = 27
            val mapped_list:Array[String] = new Array[String](total_len)
            for ((jdx, mapped_id) <- mapped_pair_array) {
              mapped_list(jdx) = mapped_id
            }
            val list_one_hot:Array[String] = new Array[String](len_one_hot)
            val list_multi_hot:Array[String] = new Array[String](total_len - len_one_hot)
            for (idx <- 0 until len_one_hot) {
              list_one_hot(idx) = mapped_list(idx)
            }
            for (idx <- 0 until (total_len - len_one_hot)) {
              list_multi_hot(idx) = mapped_list(idx + len_one_hot)
            }
            (sid.toString, list_one_hot.mkString(";") + "\t" + list_multi_hot.mkString(";"))
        })

        println("value_rdd_join_reduced_compact_count:" + value_rdd_join_reduced_compact.count)

        val info_rdd = sc.textFile(tf_text).map(
          rs => {
            val line_list = rs.split("\t")
            val sid = line_list(0)
            val label = line_list(1)
            val label_arr = line_list(2)
            val idx0 = line_list(4)
            val idx1 = line_list(5)
            val idx2 = line_list(6)
            val dense = line_list(3).split(";")
            val info = Array(dense(25), label, label_arr, idx0, idx1, idx2)
            (sid, info.mkString("\t"))
          }
        )
        println("info_rdd_count:" + info_rdd.count)

        val ult_rdd = info_rdd.join(value_rdd_join_reduced_compact)
        println("ult_rdd_count:" + ult_rdd.count)
        ult_rdd.map({
          case(sid, (info, mapped_values)) =>
            //sid + info = Array(label, label_arr, idx0, idx1, idx2) + mapped_values
            sid + "\t" + info + "\t" + mapped_values
        }).repartition(1000).saveAsTextFile(tf_text_mapped_others)
      }
    }
    println("Done.......")
    return


    /************do id map and sampling************************/
    val negativeSampleRatio = 0.19
    println("Down Sampling")
    //val info = Array(sid, label, label_arr, idx0, idx1, idx2, dense, idx_arr)
    val schema_new = StructType(List(
      StructField("sample_idx", LongType, nullable = true),
      StructField("label_single", FloatType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true))
    ))

    /************do sampling************************/
    println("Do sampling")
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_text_mapped = des_dir + "/" + src_date + "-text-mapped"
      val tf_text_mapped_tf = des_dir + "/" + src_date + "-text-mapped-tf"
      val tf_text_mapped_sampled_tf = des_dir + "/" + src_date + "-text-mapped-sampled-tf"
      println("tf_text_mapped:" + tf_text_mapped)
      println("tf_text_mapped_sampled_tf:" + tf_text_mapped_sampled_tf)
      if (exists_hdfs_path(tf_text_mapped) && (!exists_hdfs_path(tf_text_mapped_sampled_tf))) {
        delete_hdfs_path(tf_text_mapped_tf)
        delete_hdfs_path(tf_text_mapped_sampled_tf)
        val tf_text_mapped_collect = tf_text_mapped + "/part*"
        println("now load data frame:" + tf_text_mapped_collect)
        val text_rdd = sc.textFile(tf_text_mapped_collect).map({
            rs =>
              val rs_list = rs.split("\t")
              val sample_idx = rs_list(0).toLong
              val label = rs_list(1).toFloat
              val label_arr = rs_list(2).split(";").map(_.toLong).toSeq
              val idx0 = rs_list(3).split(";").map(_.toLong).toSeq
              val idx1 = rs_list(4).split(";").map(_.toLong).toSeq
              val idx2 = rs_list(5).split(";").map(_.toLong).toSeq
              val dense = rs_list(6).split(";").map(_.toLong).toSeq
              val idx_arr = rs_list(7).split(";").map(_.toLong).toSeq
              Row(sample_idx, label, label_arr, dense, idx0, idx1, idx2, idx_arr)
        })

        val text_rdd_count = text_rdd.count
        println(s"text_rdd_count is : $text_rdd_count")

        val text_df: DataFrame = spark.createDataFrame(text_rdd, schema_new)
        text_df.write.format("tfrecords").option("recordType", "Example").save(tf_text_mapped_tf)

        //保存count文件
        val text_df_count = text_df.count()
        println(s"text_df_count is : $text_df_count")
        var fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, text_df_count)
        s"hadoop fs -put $fileName $tf_text_mapped_tf/count" !

        val sampled_rdd = text_rdd.filter(
          rs => {
            val label = rs.getFloat(1)
            var filter = false
            if (label == 1.0 || Random.nextFloat() < math.abs(negativeSampleRatio)) {
              filter = true
            }
            filter
          }
        )

        val sampled_rdd_count = sampled_rdd.count
        println(s"sampled_rdd_count is : $sampled_rdd_count")

        //Save DataFrame as TFRecords
        val sampled_df: DataFrame = spark.createDataFrame(sampled_rdd, schema_new)
        sampled_df.write.format("tfrecords").option("recordType", "Example").save(tf_text_mapped_sampled_tf)

        //保存count文件
        val sampled_df_count = sampled_df.count()
        println(s"sampled_df_count is : $sampled_df_count")
        fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, sampled_df_count)
        s"hadoop fs -put $fileName $tf_text_mapped_sampled_tf/count" !
      }
    }
    println("Done.......")
  }
}