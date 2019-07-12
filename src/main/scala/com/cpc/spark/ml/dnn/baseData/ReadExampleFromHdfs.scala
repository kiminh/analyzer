package com.cpc.spark.ml.dnn.baseData

import org.apache.log4j.{ Level, Logger }
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.{DataFrame, Row, Column}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructField
import org.apache.spark.SparkContext
import scala.util.Random
import org.apache.spark.util.LongAccumulator

/**
  * 解析tfrecord到hdfs
  * created time : 2019/07/10 10:38
  * @author fenghuabin
  * @version 1.0
  *
  */

object ReadExampleFromHdfs {

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

  //def getColAtIndex(id:Int): Column = {
  //  col(s"column1")(id).as(s"column1_${id+1}")
  //}


  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println(
        """
          |you have to input 3 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val Array(src, des_dir, des_date, des_map_prefix, numPartitions) = args

    println(args)

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    val negativeSampleRatio = 0.19

    //var path = "hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/adlist-v4/2019-06-11-bak/"

    //Read TFRecords into DataFrame.
    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
    val importedDf0: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(src)
    //println("show")
    //importedDf0.show(10)
    println("printSchema")
    importedDf0.printSchema()
    //println("columns")
    //importedDf0.columns
    //println("dense_show")
    //importedDf0.describe("dense").show

    val decode_path = des_dir + "/" + des_date
    if (!exists_hdfs_path(decode_path)) {
      //delete_hdfs_path(decode_path)
      //importedDf0.rdd.repartition(numPartitions.toInt).saveAsTextFile(decode_path)
      importedDf0.rdd.saveAsTextFile(decode_path)
    }

    val acc = new LongAccumulator
    spark.sparkContext.register(acc)

    val sampled_rdd = importedDf0.rdd.filter(
      rs => {
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4).toString
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)
        var filter = false
        if (label_arr.head == 1L || Random.nextFloat() < math.abs(negativeSampleRatio)) {
          filter = true
        }
        filter
      }
    ).map(
      rs => {
        acc.add(1L)
        val idx2 = rs.getSeq[Long](0)
        val idx1 = rs.getSeq[Long](1)
        val idx_arr = rs.getSeq[Long](2)
        val idx0 = rs.getSeq[Long](3)
        val sample_idx = rs.getLong(4).toString
        val label_arr = rs.getSeq[Long](5)
        val dense = rs.getSeq[Long](6)

        var label = 0.0f
        if (label_arr.head == 1L) {
          label = 1.0f
        }
        Row(idx2, idx1, idx_arr, idx0, sample_idx, label_arr, label, dense)
      }
    )

    val sampled_path = des_dir + "/" + des_date + "-sampled"
    if (!exists_hdfs_path(sampled_path)) {
      sampled_rdd.saveAsTextFile(sampled_path)
    }

    val schema = StructType(List(
      StructField("idx2", ArrayType(LongType, containsNull = true)),
      StructField("idx1", ArrayType(LongType, containsNull = true)),
      StructField("id_arr", ArrayType(LongType, containsNull = true)),
      StructField("idx0", ArrayType(LongType, containsNull = true)),
      StructField("sample_idx", LongType, nullable = true),
      StructField("label", ArrayType(LongType, containsNull = true)),
      StructField("label_single", FloatType, nullable = true),
      StructField("dense", ArrayType(LongType, containsNull = true))))

    val tf_sampled_path = des_dir + "/" + des_date + "-sampled-tf"
    if (!exists_hdfs_path(tf_sampled_path) && exists_hdfs_path(sampled_path)) {
      //Save DataFrame as TFRecords
      val df_tf: DataFrame = spark.createDataFrame(sampled_rdd, schema)
      df_tf.write.format("tfrecords").option("recordType", "Example").save(tf_sampled_path)
    }

    //Read TFRecords into DataFrame.
    //The DataFrame schema is inferred from the TFRecords if no custom schema is provided.
    val importedDf1: DataFrame = spark.read.format("tfrecords").option("recordType", "Example").load(tf_sampled_path)
    importedDf1.show(3)

    //Read TFRecords into DataFrame using custom schema
    val importedDf2: DataFrame = spark.read.format("tfrecords").schema(schema).load(tf_sampled_path)
    importedDf2.show(3)



    importedDf0.createOrReplaceTempView("sql_table_name")
    val tf_decode_res = spark.sql("SELECT sample_idx, label, dense, idx0, idx1, idx2, id_arr FROM sql_table_name")


    //tf_decode_res("label")(0)

    //path = "hdfs://emr-cluster/user/cpc/fenghuabin/2019-06-11-decode"

    val map_path = des_dir + "/" + des_date + "-" + des_map_prefix
    //if (exists_hdfs_path(map_path)) {
    //  delete_hdfs_path(decode_path)
    //}
    if (!exists_hdfs_path(map_path)) {
      tf_decode_res.rdd.map(
        rs => {
          //output(0) = rs.getLong(0).toString
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

          //val output = new ArrayBuffer[String]
          for (idx <- 0 until dense.length) {
            output(idx + 2) = dense(idx).toString
          }

          for (idx <- 0 until idx_arr.length) {
            output(idx + 2 + dense.length) = idx_arr(idx).toString
          }

          //if (idx0.length != idx1.length || idx1.length != idx2.length || idx2.length != idx_arr.length) {
          //  output(30) = "invalid"
          //} else {
          //  output(30) = "correct"
          //}
          output.mkString("\t")
        }
      ).saveAsTextFile(map_path)
      //).repartition(numPartitions.toInt).saveAsTextFile(map_path)
    }

    val instancesData = des_dir + "/" + des_date + "-instances"
    //统计每个ID特征的每个取值出现的次数
    if (!exists_hdfs_path(instancesData) && exists_hdfs_path(map_path)) {
      var data = sc.parallelize(Array[(String, Int)]())
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
              yield (elem, 1)
          }
        ).reduceByKey(_ + _)
      )

      data.reduceByKey(_ + _).sortByKey().map {
        case (key, value) =>
          key + "\t" + value.toString
      }.repartition(1).saveAsTextFile(instancesData)
    }


    ////DataFrame转换成RDD
    //path = "hdfs://emr-cluster/user/cpc/fenghuabin/2019-06-11-bak-decode"
    //if (exists_hdfs_path(path)) {
    //  delete_hdfs_path(path)
    //}
    //tf_decode_res.rdd.saveAsTextFile(path)


    //val new_path = "hdfs://emr-cluster/user/cpc/fhb/adlist-v4/2019-06-11"
    //importedDf1.repartition(100).saveAs
  }
}