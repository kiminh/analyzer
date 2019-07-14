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

    val sparkConf = new SparkConf()
    sparkConf.set("spark.driver.maxResultSize", "5g")
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext




    val curr_file_src = des_dir + "/2019-06-10-tf-sampled-mapped/part-0"
    val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
    importedDf.printSchema()
    importedDf.show(3)

    return






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

    val instances_all_map = des_dir + "/" + instances_file + "-mapped"
    if (!exists_hdfs_path(instances_all_map)) {
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
        case (key, vaule) => key + "\t" + vaule.toString
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



    /************do id map and sampling************************/
    val negativeSampleRatio = 0.19
    println("do id map and sampling")
    val schema_old = StructType(List(
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true))))

    val schema = StructType(List(
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("label_single", FloatType, nullable = true),
      StructField("dense", ArrayType(LongType, containsNull = true))))

    /************do sampling************************/
    println("do sampling")
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val curr_file_src = src_dir + "/" + src_date
      val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled"
      println("curr_file_src:" + curr_file_src)
      println("tf_sampled_path:" + tf_sampled_path)
      if (exists_hdfs_path(curr_file_src) && (!exists_hdfs_path(tf_sampled_path))) {
        val curr_file_src_collect = curr_file_src + "/part*"
        println("now load data frame:" + curr_file_src_collect)
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src_collect)
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src_collect)
        importedDf.printSchema()
        importedDf.show(1)
        if (importedDf.count() > 0) {
          val sampled_rdd = importedDf.rdd.filter(
            rs => {
              val label_arr = rs.getSeq[Long](5)
              var filter = false
              if (label_arr.head == 1L || Random.nextFloat() < math.abs(negativeSampleRatio)) {
                filter = true
              }
              filter
            }
          )
          val sampled_rdd_count = sampled_rdd.count
          println(s"sampled_rdd_count is : $sampled_rdd_count")

          //Save DataFrame as TFRecords
          val sampled_df: DataFrame = spark.createDataFrame(sampled_rdd, schema_old)
          sampled_df.write.format("tfrecords").option("recordType", "Example").save(tf_sampled_path)

          //保存count文件
          val sampled_df_count = sampled_df.count()
          println(s"sampled_df_count is : $sampled_df_count")
          val fileName = "count_" + Random.nextInt(100000)
          writeNum2File(fileName, sampled_df_count)
          s"hadoop fs -put $fileName $tf_sampled_path/count" !
        }
      }
    }

    /************do mapping************************/
    println("do mapping")
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled"
      val tf_mapped_path = des_dir + "/" + src_date + "-tf-sampled-mapped"
      if (!exists_hdfs_path(tf_mapped_path + "/count") && exists_hdfs_path(tf_sampled_path)) {
        //Read TFRecords into DataFrame using custom schema

        s"hadoop fs -rm -r $tf_mapped_path" !

        val src_tail_collect :Array[String] = new Array[String](10)
        src_tail_collect(0) = "/part-r-000*"
        src_tail_collect(1) = "/part-r-001*"
        src_tail_collect(2) = "/part-r-002*"
        src_tail_collect(3) = "/part-r-003*"
        src_tail_collect(4) = "/part-r-004*"
        src_tail_collect(5) = "/part-r-005*"
        src_tail_collect(6) = "/part-r-006*"
        src_tail_collect(7) = "/part-r-007*"
        src_tail_collect(8) = "/part-r-008*"
        src_tail_collect(9) = "/part-r-009*"

        var total_count = 0L
        for (idx <- src_tail_collect.indices) {
          val tf_mapped_path_part = tf_mapped_path + "/part-" + idx.toString
          val tf_sampled_path_collect = tf_sampled_path + src_tail_collect(idx)
          val importedDf1: DataFrame = spark.read.format("tfrecords").schema(schema_old).load(tf_sampled_path_collect)
          println("DF file count:" + importedDf1.count().toString + " of file:" + tf_sampled_path_collect)
          importedDf1.printSchema()
          importedDf1.show(1)

          val mapped_sampled_rdd = importedDf1.rdd.map(
            rs => {
              val idx2 = rs.getSeq[Long](0)
              val idx1 = rs.getSeq[Long](1)
              val idx_arr = rs.getSeq[Long](2)
              val idx0 = rs.getSeq[Long](3)
              val sample_idx = rs.getLong(4)
              val label_arr = rs.getSeq[Long](5)
              val dense = rs.getSeq[Long](6)

              var label = 0.0f
              if (label_arr.head == 1L) {
                label = 1.0f
              }

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

              Row(idx2, idx1, idx_arr_mapped, idx0, sample_idx, label_arr, label, dense_mapped_seq)
            }
          )

          val mapped_sampled_rdd_count = mapped_sampled_rdd.count
          println(s"mapped_sampled_rdd_count is : $mapped_sampled_rdd_count")

          //Save DataFrame as TFRecords
          val mapped_sampled_df: DataFrame = spark.createDataFrame(mapped_sampled_rdd, schema)
          mapped_sampled_df.write.format("tfrecords").option("recordType", "Example").save(tf_mapped_path_part)

          val mapped_sampled_df_count = mapped_sampled_df.count()
          println(s"mapped_sampled_df_count is : $mapped_sampled_df_count")
          total_count += mapped_sampled_df_count
        }

        //保存count文件
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, total_count)
        s"hadoop fs -put $fileName $tf_mapped_path/count" !
      }
    }

  }
}