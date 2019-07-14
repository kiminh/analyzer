package com.cpc.spark.ml.dnn.baseData

import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.sys.process._
import scala.util.Random
import org.apache.spark.util.LongAccumulator

/**
  * 解析tfrecord到hdfs
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
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val src_date_list = src_date_str.split(";")
    println("collect map instances for id feature")
    /************collect map instances for id feature************************/
    for (src_date <- src_date_list) {
      val instances_path = des_dir + "/instances-" + src_date
      if (!exists_hdfs_path(instances_path)) {
        val curr_file_src = src_dir + "/" + src_date + "/part-r-*"
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
        //importedDf.cache()
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src)
        if (importedDf.count() < 10000) {
          println("invalid df count, df file:" + curr_file_src)
        } else {
          val map_path = des_dir + "/instances-" + src_date + "-collect"
          if (!exists_hdfs_path(map_path)) {
            val date_token = src_date.split("-")
            val viewName = "sql_table_view_name_" + date_token.mkString("_")
            println("viewName:" + viewName)
            importedDf.createOrReplaceTempView(viewName)
            val tf_decode_df_rows = spark.sql("SELECT sample_idx, label, dense, idx0, idx1, idx2, id_arr FROM " + viewName)
            tf_decode_df_rows.rdd.map(
              rs => {
                val sample_idx = rs.getLong(0).toString
                val label_arr = rs.getSeq[Long](1)
                val dense = rs.getSeq[Long](2)
                val idx0 = rs.getSeq[Long](3)
                val idx1 = rs.getSeq[Long](4)
                val idx2 = rs.getSeq[Long](5)
                val idx_arr = rs.getSeq[Long](6)

                val output: Array[String] = new Array[String](1 + 1 + dense.length + idx_arr.length)

                var label = "0.0"
                if (label_arr.head == 1) {
                  label = "1.0"
                } else {
                  label = "0.0"
                }
                output(0) = sample_idx
                output(1) = label

                for (idx <- 0 until dense.length) {
                  output(idx + 2) = dense(idx).toString
                }

                for (idx <- 0 until idx_arr.length) {
                  output(idx + 2 + dense.length) = idx_arr(idx).toString
                }

                output.mkString("\t")
              }
            ).saveAsTextFile(map_path)
          }

          var data = sc.parallelize(Array[(String, Long)]())
          data = data.union(
            sc.textFile(map_path).map(
              rs => {
                val line = rs.split("\t")
                val output: Array[String] = new Array[String](line.length - 2)
                for (idx <- 2 until line.length) {
                  output(idx - 2) = line(idx)
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
          )

          data.reduceByKey(_ + _).sortByKey().map {
            case (key, value) =>
              key + "\t" + value.toString
          }.repartition(1).saveAsTextFile(instances_path)
        }
      }
    }

    println("make total instances")
    /************************make total instances********************************/
    val output = scala.collection.mutable.ArrayBuffer[String]()
    for (src_date <- src_date_list) {
      val instances_path = des_dir + "/instances-" + src_date
      if (exists_hdfs_path(instances_path)) {
        output += instances_path
      }
    }

    val src_instances_files = output.mkString(",")
    //统计每个ID特征的每个取值出现的次数
    val instances_all = des_dir + "/" + instances_file
    if (!exists_hdfs_path(instances_all)) {
      var data = sc.parallelize(Array[(String, Long)]())
      data = data.union(
        sc.textFile(src_instances_files).map(
          rs => {
            val line = rs.split("\t")
            val key = line(0)
            val cnt = line(1).toLong
            (key, cnt)
          }
        ).reduceByKey(_ + _)
      )
      data.reduceByKey(_ + _).repartition(1).sortBy(_._2 * -1).map {
        case (key, value) =>
          key + "\t" + value.toString
      }.saveAsTextFile(instances_all)
    }

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    val instances_all_map = des_dir + "/" + instances_file

    if (!exists_hdfs_path(instances_all_map)) {
      sc.textFile(instances_all).map{
        rs => {
          acc.add(1L)
          val line = rs.split("\t")
          val key = line(0)
          key + "\t" + acc.sum.toString
        }
      }.saveAsTextFile(instances_all_map)
    }

    /************************load map********************************/
    println("load sparseMap")
    val sparseMap = sc.textFile(instances_all_map).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0).toLong
        val key = line(1).toLong - 1L
        (field, key)
      }
    }.collectAsMap()

    println("sparseMap.size=" + sparseMap.size)

    val negativeSampleRatio = 0.19
    val schema = StructType(List(
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true)),
      StructField("id_arr_mapped", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("label_single", FloatType, nullable = true),
      StructField("dense", ArrayType(LongType, containsNull = true)),
      StructField("dense_mapped", ArrayType(LongType, containsNull = true))))

    println("do id map and sampling")
    /************do id map and sampling************************/
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val curr_file_src = src_dir + "/" + src_date + "/part-r-*"
      val tf_mapped_path = des_dir + "/" + src_date + "-tf-mapped"
      if (!exists_hdfs_path(tf_mapped_path) && exists_hdfs_path(curr_file_src)) {
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src)

        val mapped_rdd = importedDf.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx0 = rs.getSeq[Long](3)
            val sample_idx = rs.getLong(4)
            val label_arr = rs.getSeq[Long](5)
            val dense = rs.getSeq[Long](6)

            val dense_mapped: Array[Long] = new Array[Long](dense.length)
            for (idx <- dense.indices) {
              dense_mapped(idx) = sparseMap.getOrElse(dense(idx), 0L)
            }
            val dense_mapped_seq: Seq[Long] = dense_mapped

            val idx_arr_mapped: Array[Long] = new Array[Long](idx_arr.length)
            for (idx <- idx_arr.indices) {
              idx_arr_mapped(idx) = sparseMap.getOrElse(idx_arr(idx), 0L)
            }
            val idx_arr_mapped_seq: Seq[Long] = idx_arr_mapped

            var label = 0.0f
            if (label_arr.head == 1L) {
              label = 1.0f
            }
            Row(idx2, idx1, idx_arr, idx_arr_mapped_seq, idx0, sample_idx, label_arr, label, dense, dense_mapped_seq)
          }
        )

        val mapped_rdd_count = mapped_rdd.count
        println(s"mapped_rdd_count is : $mapped_rdd_count")
        //Save DataFrame as TFRecords
        val df_tf: DataFrame = spark.createDataFrame(mapped_rdd, schema)
        df_tf.write.format("tfrecords").option("recordType", "Example").save(tf_mapped_path)

        //保存count文件
        val mapped_tf_df_count = df_tf.count()
        println(s"mapped_tf_df_count is : $mapped_tf_df_count")
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, mapped_tf_df_count)
        s"hadoop fs -put $fileName $tf_mapped_path/count" !
      }

      val tf_sampled_mapped_path = des_dir + "/" + src_date + "-tf-mapped-sampled"
      if (!exists_hdfs_path(tf_sampled_mapped_path)) {
        //Read TFRecords into DataFrame using custom schema
        val importedDf1: DataFrame = spark.read.format("tfrecords").schema(schema).load(tf_mapped_path)
        println("DF file count:" + importedDf1.count().toString + " of file:" + tf_mapped_path)
        importedDf1.printSchema()
        importedDf1.show(1)
        val sampled_mapped_rdd = importedDf1.rdd.map(
          rs => {
            val idx2 = rs.getSeq[Long](0)
            val idx1 = rs.getSeq[Long](1)
            val idx_arr = rs.getSeq[Long](2)
            val idx_arr_mapped = rs.getSeq[Long](3)
            val idx0 = rs.getSeq[Long](5)
            val sample_idx = rs.getLong(5)
            val label_arr = rs.getSeq[Long](6)
            val label = rs.getFloat(7)
            val dense = rs.getSeq[Long](8)
            val dense_mapped = rs.getSeq[Long](9)
            Row(idx2, idx1, idx_arr, idx_arr_mapped, idx0, sample_idx, label_arr, label, dense, dense_mapped)
          }
        ).filter(
          rs => {
            val label_arr = rs.getSeq[Long](6)
            var filter = false
            if (label_arr.head == 1L || Random.nextFloat() < math.abs(negativeSampleRatio)) {
              filter = true
            }
            filter
          }
        )

        val sampled_mapped_rdd_count = sampled_mapped_rdd.count
        println(s"sampled_mapped_rdd_count is : $sampled_mapped_rdd_count")

        //Save DataFrame as TFRecords
        val df_tf_sampled: DataFrame = spark.createDataFrame(sampled_mapped_rdd, schema)
        df_tf_sampled.write.format("tfrecords").option("recordType", "Example").save(tf_sampled_mapped_path)

        //保存count文件
        val sampled_mapped_tf_df_count = df_tf_sampled.count()
        println(s"sampled_mapped_tf_df_count is : $sampled_mapped_tf_df_count")
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, sampled_mapped_tf_df_count)
        s"hadoop fs -put $fileName $tf_sampled_mapped_path/count" !
      }
    }

  }
}