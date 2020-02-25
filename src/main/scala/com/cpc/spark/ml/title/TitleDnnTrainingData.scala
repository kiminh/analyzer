package com.cpc.spark.ml.title

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.commons.codec.binary.Base64
import org.apache.hadoop.io.BytesWritable
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.tensorflow.example.Example
import org.tensorflow.spark.datasources.tfrecords.TensorFlowInferSchema
import org.tensorflow.spark.datasources.tfrecords.serde.DefaultTfRecordRowDecoder

/**
  * 提取样本的hashcode
  * created time : 2020/02/09 16:38
  * @author duanguangdong
  * @version 2.0
  *
  */

object TitleDnnTrainingData {
  Logger.getRootLogger.setLevel(Level.WARN)

  def generateSql(model_name: String, curday: String, sample_path: String): String = {
    val sql = s"select example from dl_cpc.$sample_path where dt='$curday' and task='$model_name'"
    sql
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println(
        """
          |you have to input 3 parameters !!!
        """.stripMargin)
      System.exit(1)
    }
    val model_name = args(0)
    val sample_path = args(1)
    val curday = args(2)
    println(args)
    val spark = SparkSession.builder().appName("embedding replace").enableHiveSupport().getOrCreate()
    import spark.implicits._
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    var importedDf: DataFrame = null

    if (sample_path.startsWith("hdfs://")) {
      importedDf = spark.read.format("tfrecords").option("recordType", "Example").load(sample_path).repartition(3000)
    } else {
      val rdd = spark.sql(generateSql(model_name, oneday, sample_path))
        .rdd.map(x => Base64.decodeBase64(x.getString(0)))
        .filter(_ != null)
      val exampleRdd = rdd.map(x => Example.parseFrom(new BytesWritable(x).getBytes))
      val finalSchema = TensorFlowInferSchema(exampleRdd)
      val rowRdd = exampleRdd.map(example => DefaultTfRecordRowDecoder.decodeExample(example, finalSchema))
      importedDf = spark.createDataFrame(rowRdd, finalSchema).repartition(3000)
    }

    println("file read finish")
    importedDf.createOrReplaceTempView("training_data")
    val title_embedding = spark.sparkContext.textFile("/user/cpc/dgd/title/title_embed.txt").map {
      r =>
        val list = r.split(";")
        val ideaid = list(0)
        val embed = list(1).substring(1, list(1).length-1)
        val embed_list = embed.split(", ")
        var embed_arr = Seq[Float]()
        for(i <- embed_list){
          embed_arr = embed_arr :+ i.toFloat
      }
        (ideaid, embed_arr)
    }.toDF("ideaid", "title")


    title_embedding.select(hash("f11")($"ideaid").alias("ideaidhash"), $"title").createOrReplaceTempView("idea_table")

    val old_ideaid_data = spark.sql(
      s"""
         |select t1.idx2, t1.idx1, t1.id_arr, t1.idx0, t1.sample_idx, t1.label, t1.dense, t2.title as floats
         |from training_data t1 left join idea_table t2 on t1.dense[11]=t2.ideaidhash
         |""".stripMargin)

    old_ideaid_data.repartition(1000).select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense", $"floats").write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/title/$oneday/")
    val CountPathTmpNamev4 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/title/tmp/"
    val CountPathNamev4 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/title/$oneday/count"
    val old_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/oldad/$oneday/part*")
    CommonUtils.writeCountToFile(spark, old_sample.count(), CountPathTmpNamev4, CountPathNamev4)

  }
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + "#" + num, 0) else Murmur3Hash.stringHash64(prefix + "#", 0)
  }
}