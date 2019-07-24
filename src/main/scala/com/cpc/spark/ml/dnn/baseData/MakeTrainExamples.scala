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



    //val curr_file_src = des_dir + "/2019-06-10-tf-sampled-mapped/part-0"
    //val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
    //importedDf.printSchema()
    //importedDf.show(3)
    //return




    val src_date_list = src_date_str.split(";")
    println("Decode instances for id feature from TF")
    /************Decode instances for id feature from TF************************/
    for (src_date <- src_date_list) {
      val map_path = des_dir + "/instances/instances-" + src_date + "-collect"
      if (!exists_hdfs_path(map_path)) {
        val curr_file_src = src_dir + "/" + src_date + "/part-r-*"
        val importedDf: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(curr_file_src)
        //importedDf.cache()
        println("DF file count:" + importedDf.count().toString + " of file:" + curr_file_src)
        if (importedDf.count() < 10000) {
          println("Invalid df count, df file:" + curr_file_src)
        } else {
          val date_token = src_date.split("-")
          val viewName = "sql_table_view_name_" + date_token.mkString("_")
          println("ViewName:" + viewName)
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
              for (idx <- dense.indices) {
                output(idx + 2) = dense(idx).toString
              }
              for (idx <- idx_arr.indices) {
                output(idx + 2 + dense.length) = idx_arr(idx).toString
              }
              output.mkString("\t")
            }
          ).saveAsTextFile(map_path)
        }
      }
    }

    println("Collect instances for uid and other features")
    /************Decode instances for id feature from TF************************/
    for (src_date <- src_date_list) {
      val map_path = des_dir + "/instances/instances-" + src_date + "-collect"
      val instances_path = des_dir + "/instances/instances-" + src_date
      if (!exists_hdfs_path(instances_path)) {
        var data = sc.parallelize(Array[(String, Long)]())
        data = data.union(
          sc.textFile(map_path).map(
            rs => {
              val line = rs.split("\t")
              val output = ArrayBuffer[String]()
              val uid_idx = 27
              for (idx <- 2 until line.length) {
                if (idx != uid_idx) {
                  output += line(idx)
                }
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

      val instances_path_uid = des_dir + "/instances/instances-" + src_date + "-uid"
      if (!exists_hdfs_path(instances_path_uid)) {
        var data = sc.parallelize(Array[(String, Long)]())
        data = data.union(
          sc.textFile(map_path).map(
            rs => {
              val line = rs.split("\t")
              val uid_idx = 27
              (line(uid_idx), 1L)
            }
          ).reduceByKey(_ + _)
        )
        data.reduceByKey(_ + _).sortByKey().map {
          case (key, value) =>
            key + "\t" + value.toString
        }.repartition(1).saveAsTextFile(instances_path_uid)
      }
    }

    println("Make total instances for uid")
    /************************make total instances for uid********************************/
    val output_uid = scala.collection.mutable.ArrayBuffer[String]()
    val output = scala.collection.mutable.ArrayBuffer[String]()
    for (src_date <- src_date_list) {
      val instances_path = des_dir + "/instances/instances-" + src_date
      val instances_path_uid = des_dir + "/instances/instances-" + src_date + "-uid"
      if (exists_hdfs_path(instances_path)) {
        output += instances_path
      }
      if (exists_hdfs_path(instances_path_uid)) {
        output_uid += instances_path_uid
      }
    }

    val src_instances_files_uid = output_uid.mkString(",")
    //统计uid特征的每个取值出现的次数
    val instances_all_uid = des_dir + "/instances/" + instances_file + "-uid"
    if (!exists_hdfs_path(instances_all_uid)) {
      var data = sc.parallelize(Array[(String, Long)]())
      data = data.union(
        sc.textFile(src_instances_files_uid).map(
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
      }.saveAsTextFile(instances_all_uid)
    }

    val instances_all_map_uid = des_dir + "/instances/" + instances_file + "-uid-mapped"
    if (!exists_hdfs_path(instances_all_map_uid)) {
      val acc = new LongAccumulator
      spark.sparkContext.register(acc)
      sc.textFile(instances_all_uid).coalesce(1, false).map{
        rs => {
          acc.add(1L)
          val line = rs.split("\t")
          val key = line(0)
          (key, acc.count)
        }
      }.repartition(1).sortBy(_._2).map{
        case (key, vaule) => key + "\t" + vaule.toString
      }.saveAsTextFile(instances_all_map_uid)
    }

    println("Make total instances for other features")
    /************************make total instances for other featuires********************************/
    val src_instances_files = output.mkString(",")
    //统计所有ID特征的每个取值出现的次数
    val instances_all = des_dir + "/instances/" + instances_file
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

    val instances_all_map = des_dir + "/instances/" + instances_file + "-mapped"
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

    /************check sid************************/
    //println("Check sample index")
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

    /************************load map********************************/
    println("load other sparseMap")
    val sparseMap = sc.textFile(instances_all_map).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0).toLong
        val key = (line(1).toLong - 1L).toString
        (field, key)
      }
    }.collectAsMap()
    println("sparseMap.size=" + sparseMap.size)
    val sparse_size = sparseMap.size.toLong
    val sparse_size_bc = sc.broadcast(sparse_size)
    val sparseMapBC = sc.broadcast(sparseMap)

    /************************load map********************************/
    println("load uid sparseMap")
    val sparseMapUid = sc.textFile(instances_all_map_uid).map{
      rs => {
        val line = rs.split("\t")
        val field = line(0).toLong
        val key = (line(1).toLong - 1L).toString
        (field, key)
      }
    }.collectAsMap()
    println("sparseMapUid.size=" + sparseMapUid.size)
    val sparse_size_uid = sparseMapUid.size.toLong
    val sparse_size_uid_bc = sc.broadcast(sparse_size_uid)
    val sparseMapUidBC = sc.broadcast(sparseMapUid)

    val sparse_size_total = sparse_size + sparse_size_uid
    val sparse_size_total_bc = sc.broadcast(sparse_size_total)


    /************do mapping************************/
    println("Do mapping uid feature")
    for (src_date <- src_date_list) {
      val tf_plain_mapped_path_uid = des_dir + "/" + src_date + "-text-mapped-uid"
      val tf_text = des_dir + "/" + src_date + "-text"
      if (!exists_hdfs_path(tf_plain_mapped_path_uid) && exists_hdfs_path(tf_text)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        println("make " + tf_plain_mapped_path_uid)
        sc.textFile(tf_text).map(
          rs => {
            output.mkString("\t")
            val line_list = rs.split("\t")
            val dense = line_list(3).split(";")
            val uid_idx = 25
            val dense_other = scala.collection.mutable.ArrayBuffer[String]()
            for (idx <- dense.indices) {
              if (idx != uid_idx) {
                dense_other += dense(idx)
              }
            }
            val uid_value = dense(uid_idx).toLong
            val mapped_uid_value = sparse_size_bc.value + sparseMapUid.getOrElse(uid_value.toLong, sparse_size_total_bc.value.toString)
            line_list(3) = dense_other.mkString(";")
            mapped_uid_value + "\t" + line_list.mkString("\t")
          }
        ).saveAsTextFile(tf_plain_mapped_path_uid)
      }
    }
    println("Done.......")

    println("Do mapping other features")
    for (src_date <- src_date_list) {
      val tf_plain_mapped_path = des_dir + "/" + src_date + "-text-mapped"
      val tf_plain_mapped_path_uid = des_dir + "/" + src_date + "-text-mapped-uid"
      if (!exists_hdfs_path(tf_plain_mapped_path) && exists_hdfs_path(tf_plain_mapped_path_uid)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        sc.textFile(tf_plain_mapped_path_uid).map(
          rs => {
            val line_list = rs.split("\t")
            //val uid = line_list(0)
            //val sid = line_list(1)
            //val label = line_list(2)
            //val label_arr = line_list(3)
            val dense = line_list(4).split(";")
            //val idx0 = line_list(5)
            //val idx1 = line_list(6)
            //val idx2 = line_list(7)
            val idx_arr = line_list(8).split(";")

            val idx_arr_list = idx_arr.map(x => sparseMapBC.value.getOrElse(x.toLong, sparse_size_total_bc.value).toString)
            val dense_list = dense.map(x => sparseMapBC.value.getOrElse(x.toLong, sparse_size_total_bc.value).toString)
            line_list(4) = dense_list.mkString(";")
            line_list(8) = idx_arr_list.mkString(";")

            line_list.mkString("\t")
          }
        ).saveAsTextFile(tf_plain_mapped_path)

      }
    }
    println("Done.......")

    return
    /************do mapping************************/
    /**println("do plain mapping")
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled-plain"
      val tf_mapped_path = des_dir + "/" + src_date + "-tf-sampled-plain-mapped-scala"
      if (exists_hdfs_path(tf_sampled_path)) {

        val src_tail_collect :Array[String] = new Array[String](10)
        src_tail_collect(0) = "/part-00000"
        src_tail_collect(1) = "/part-00001"
        src_tail_collect(2) = "/part-00002"
        src_tail_collect(3) = "/part-00003"
        src_tail_collect(4) = "/part-00004"
        src_tail_collect(5) = "/part-00005"
        src_tail_collect(6) = "/part-00006"
        src_tail_collect(7) = "/part-00007"
        src_tail_collect(8) = "/part-00008"
        src_tail_collect(9) = "/part-00009"

        for (idx <- src_tail_collect.indices) {
          val tf_mapped_path_part = tf_mapped_path + "/part-" + idx.toString
          if (!exists_hdfs_path(tf_mapped_path_part)) {
            val tf_sampled_path_collect = tf_sampled_path + src_tail_collect(idx)

            sc.textFile(tf_sampled_path_collect).map(
              rs => {
                //output += sample_idx.toString
                //output += label
                //output += label_arr.mkString(";")
                //output += dense.mkString(";")
                //output += idx0.mkString(";")
                //output += idx1.mkString(";")
                //output += idx2.mkString(";")
                //output += idx_arr.mkString(";")
                //output.mkString("\t")
                //val idx2 = rs_list(6).split(";").map(_.toLong).toSeq

                val line_list = rs.split("\t")
                val dense_list = line_list(3).split(";").map(x => sparseMap.getOrElse(x.toLong, sparse_size_bc.value.toString)).toSeq
                val idx_arr_list = line_list(7).split(";").map(x => sparseMap.getOrElse(x.toLong, sparse_size_bc.value.toString)).toSeq

                line_list(3) = dense_list.mkString(";")
                line_list(7) = idx_arr_list.mkString(";")
                line_list.mkString("\t")
              }
            ).saveAsTextFile(tf_mapped_path_part)

          }
        }
      }
    }**/

    /************do id map and sampling************************/
    val negativeSampleRatio = 0.19
    println("Do id map and sampling")
    val schema_old = StructType(List(
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("dense", ArrayType(LongType, containsNull = true))))

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
    println("Done.......")



    /************get plain sampled examples************************/
    println("Do make plain sampled examples")
    for (src_date <- src_date_list) {
      val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled"
      val tf_plain_path = des_dir + "/" + src_date + "-tf-sampled-plain"
      if (!exists_hdfs_path(tf_plain_path) && exists_hdfs_path(tf_sampled_path)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        val tf_sampled_path_collect = tf_sampled_path + "/part*"
        val importedDf1: DataFrame = spark.read.format("tfrecords").schema(schema_old).load(tf_sampled_path_collect)
        println("DF file count:" + importedDf1.count().toString + " of file:" + tf_sampled_path_collect)
        importedDf1.printSchema()
        importedDf1.show(3)
        importedDf1.rdd.map(
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
            output += label_arr.mkString(";")
            output += dense.mkString(";")
            output += idx0.mkString(";")
            output += idx1.mkString(";")
            output += idx2.mkString(";")
            output += idx_arr.mkString(";")

            output.mkString("\t")
          }
        ).repartition(10).saveAsTextFile(tf_plain_path)
      }
    }
    println("Done.......")

    /************get plain sampled examples************************/
    println("Do make small plain sampled examples")
    for (src_date <- src_date_list) {
      val tf_plain_path = des_dir + "/" + src_date + "-tf-sampled-plain"
      val tf_plain_path_small = des_dir + "/" + src_date + "-tf-sampled-plain-small"
      if (!exists_hdfs_path(tf_plain_path_small) && exists_hdfs_path(tf_plain_path)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        sc.textFile(tf_plain_path).repartition(400).saveAsTextFile(tf_plain_path_small)
      }
    }
    println("Done.......")

    ///************do plain tf trans************************/
    println("Trans plain mapped to tf")
    for (src_date <- src_date_list) {
      val tf_plain_mapped_path = des_dir + "/" + src_date + "-tf-sampled-plain-mapped"
      val tf_trans_path = des_dir + "/" + src_date + "-tf-sampled-plain-mapped-trans"
      if (!exists_hdfs_path(tf_trans_path) && exists_hdfs_path(tf_plain_mapped_path)) {
        println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
        val trans_rdd = sc.textFile(tf_plain_mapped_path).map(
          rs => {
            val rs_list = rs.split("\t")
            val sample_idx = rs_list(0).toLong
            val label = rs_list(1).toFloat
            val label_arr = rs_list(2).split(";").map(_.toLong).toSeq
            val dense = rs_list(3).split(";").map(_.toLong).toSeq
            val idx0 = rs_list(4).split(";").map(_.toLong).toSeq
            val idx1 = rs_list(5).split(";").map(_.toLong).toSeq
            val idx2 = rs_list(6).split(";").map(_.toLong).toSeq
            val idx_arr = rs_list(7).split(";").map(_.toLong).toSeq

            Row(sample_idx, label, label_arr, dense, idx0, idx1, idx2, idx_arr)
          }
        ).repartition(300)

        val trans_rdd_count = trans_rdd.count
        println(s"trans_rdd_count is : $trans_rdd_count")

        //Save DataFrame as TFRecords
        val trans_df: DataFrame = spark.createDataFrame(trans_rdd, schema_new)
        trans_df.write.format("tfrecords").option("recordType", "Example").save(tf_trans_path)
        trans_df.printSchema()
        trans_df.show(3)

        val trans_df_count = trans_df.count()
        println(s"trans_df_count is : $trans_df_count")

        //保存count文件
        val fileName = "count_" + Random.nextInt(100000)
        writeNum2File(fileName, trans_df_count)
        s"hadoop fs -put $fileName $tf_trans_path/count" !
      }
    }
    println("Done.......")

    return


    /************do mapping************************/
    /*println("do plain mapping")
    for (src_date <- src_date_list) {
      println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
      val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled-plain"
      val tf_mapped_path = des_dir + "/" + src_date + "-tf-sampled-plain-mapped-scala"
      if (exists_hdfs_path(tf_sampled_path)) {

        val src_tail_collect :Array[String] = new Array[String](10)
        src_tail_collect(0) = "/part-00000"
        src_tail_collect(1) = "/part-00001"
        src_tail_collect(2) = "/part-00002"
        src_tail_collect(3) = "/part-00003"
        src_tail_collect(4) = "/part-00004"
        src_tail_collect(5) = "/part-00005"
        src_tail_collect(6) = "/part-00006"
        src_tail_collect(7) = "/part-00007"
        src_tail_collect(8) = "/part-00008"
        src_tail_collect(9) = "/part-00009"

        for (idx <- src_tail_collect.indices) {
          val tf_mapped_path_part = tf_mapped_path + "/part-" + idx.toString
          if (!exists_hdfs_path(tf_mapped_path_part)) {
            val tf_sampled_path_collect = tf_sampled_path + src_tail_collect(idx)

            sc.textFile(tf_sampled_path_collect).map(
              rs => {
                //output += sample_idx.toString
                //output += label
                //output += label_arr.mkString(";")
                //output += dense.mkString(";")
                //output += idx0.mkString(";")
                //output += idx1.mkString(";")
                //output += idx2.mkString(";")
                //output += idx_arr.mkString(";")
                //output.mkString("\t")
                //val idx2 = rs_list(6).split(";").map(_.toLong).toSeq

                val line_list = rs.split("\t")
                val dense_list = line_list(3).split(";").map(x => sparseMap.getOrElse(x.toLong, sparse_size_bc.value.toString)).toSeq
                val idx_arr_list = line_list(7).split(";").map(x => sparseMap.getOrElse(x.toLong, sparse_size_bc.value.toString)).toSeq

                line_list(3) = dense_list.mkString(";")
                line_list(7) = idx_arr_list.mkString(";")
                line_list.mkString("\t")
              }
            ).saveAsTextFile(tf_mapped_path_part)

          }
        }
      }
    }*/



    ///************************load map********************************/
    //println("load sparseMap")
    //val sparseMap = sc.textFile(instances_all_map).map{
    //  rs => {
    //    val line = rs.split("\t")
    //    val field = line(0).toLong
    //    val key = line(1).toLong - 1L
    //    (field, key)
    //  }
    //}.collectAsMap()
    //println("sparseMap.size=" + sparseMap.size)

    ///************do mapping************************/
    //println("do mapping")
    //for (src_date <- src_date_list) {
    //  println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    //  val tf_sampled_path = des_dir + "/" + src_date + "-tf-sampled"
    //  val tf_mapped_path = des_dir + "/" + src_date + "-tf-sampled-mapped"
    //  if (exists_hdfs_path(tf_sampled_path)) {
    //    //Read TFRecords into DataFrame using custom schema

    //    val src_tail_collect :Array[String] = new Array[String](10)
    //    src_tail_collect(0) = "/part-r-000*"
    //    src_tail_collect(1) = "/part-r-001*"
    //    src_tail_collect(2) = "/part-r-002*"
    //    src_tail_collect(3) = "/part-r-003*"
    //    src_tail_collect(4) = "/part-r-004*"
    //    src_tail_collect(5) = "/part-r-005*"
    //    src_tail_collect(6) = "/part-r-006*"
    //    src_tail_collect(7) = "/part-r-007*"
    //    src_tail_collect(8) = "/part-r-008*"
    //    src_tail_collect(9) = "/part-r-009*"

    //    for (idx <- src_tail_collect.indices) {
    //      val tf_mapped_path_part = tf_mapped_path + "/part-" + idx.toString
    //      if (!exists_hdfs_path(tf_mapped_path_part) || !exists_hdfs_path(tf_mapped_path_part + "/count")) {
    //        s"hadoop fs -rm -r $tf_mapped_path_part" !
    //        val tf_sampled_path_collect = tf_sampled_path + src_tail_collect(idx)
    //        val importedDf1: DataFrame = spark.read.format("tfrecords").schema(schema_old).load(tf_sampled_path_collect)
    //        println("DF file count:" + importedDf1.count().toString + " of file:" + tf_sampled_path_collect)
    //        importedDf1.printSchema()
    //        importedDf1.show(3)

    //        val mapped_sampled_rdd = importedDf1.rdd.map(
    //          rs => {
    //            val idx2 = rs.getSeq[Long](0)
    //            val idx1 = rs.getSeq[Long](1)
    //            val idx_arr = rs.getSeq[Long](2)
    //            val idx0 = rs.getSeq[Long](3)
    //            val sample_idx = rs.getLong(4)
    //            val label_arr = rs.getSeq[Long](5)
    //            val dense = rs.getSeq[Long](6)

    //            var label = 0.0f
    //            if (label_arr.head == 1L) {
    //              label = 1.0f
    //            }

    //            val dense_mapped: Array[Long] = new Array[Long](dense.length)
    //            for (idx <- dense.indices) {
    //              dense_mapped(idx) = sparseMap.getOrElse(dense(idx), 0L)
    //            }
    //            val dense_mapped_seq: Seq[Long] = dense_mapped

    //            val idx_arr_mapped: Array[Long] = new Array[Long](idx_arr.length)
    //            for (idx <- idx_arr.indices) {
    //              idx_arr_mapped(idx) = sparseMap.getOrElse(idx_arr(idx), 0L)
    //            }
    //            val idx_arr_mapped_seq: Seq[Long] = idx_arr_mapped

    //            Row(sample_idx, label, label_arr, dense_mapped_seq, idx0, idx1, idx2, idx_arr_mapped_seq)
    //          }
    //        )

    //        //val mapped_sampled_rdd_count = mapped_sampled_rdd.count
    //        //println(s"mapped_sampled_rdd_count is : $mapped_sampled_rdd_count")

    //        //Save DataFrame as TFRecords
    //        val mapped_sampled_df: DataFrame = spark.createDataFrame(mapped_sampled_rdd, schema_new)
    //        mapped_sampled_df.write.format("tfrecords").option("recordType", "Example").save(tf_mapped_path_part)
    //        mapped_sampled_df.printSchema()
    //        mapped_sampled_df.show(3)

    //        val mapped_sampled_df_count = mapped_sampled_df.count()
    //        println(s"mapped_sampled_df_count is : $mapped_sampled_df_count")

    //        //保存count文件
    //        val fileName = "count_" + Random.nextInt(100000)
    //        writeNum2File(fileName, mapped_sampled_df_count)
    //        s"hadoop fs -put $fileName $tf_mapped_path_part/count" !
    //      }
    //    }

    //  }
    //}

  }
}