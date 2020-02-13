package com.cpc.spark.ml.dnn.baseData

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

object ReplacedDnnTrainingData {
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

    val advDBProp = new Properties()
    val advDBUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    advDBProp.put("user", "adv_live_read")
    advDBProp.put("password", "seJzIPUc7xU")
    advDBProp.put("driver", "com.mysql.jdbc.Driver")

    val idea = s"""
                  |(SELECT distinct id as ideaid, category FROM adv.`idea` where create_time<'$oneday' and id is not null) t """.stripMargin
    val idea_id = spark.read.jdbc(advDBUrl, idea, advDBProp).select(hash("f11")($"ideaid").alias("ideaidhash"),
      $"ideaid", hash("f11")($"category").alias("newideaidhash")).createOrReplaceTempView("idea_table")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_adclass_hashcode partition (day='$oneday')
         |select ideaidhash, newideaidhash from idea_table
         |""".stripMargin)

    val new_ideaid_data = spark.sql(
      s"""
         |select t1.idx2, t1.idx1, t1.id_arr, t1.idx0, t1.sample_idx, t1.label,
         |array(t1.dense[0],t1.dense[1],t1.dense[2],t1.dense[3],t1.dense[4],t1.dense[5],t1.dense[6],
         |t1.dense[7],t1.dense[8],t1.dense[9],t1.dense[10],if(t2.ideaid is null,t2.newideaidhash, t1.dense[11]),
         |t1.dense[12],t1.dense[13],t1.dense[14],t1.dense[15],t1.dense[16],t1.dense[17],t1.dense[18],t1.dense[19],
         |t1.dense[20],t1.dense[21],t1.dense[22],t1.dense[23],t1.dense[24],t1.dense[25],t1.dense[26],t1.dense[27],
         |t1.dense[28],t1.dense[29],t1.dense[30],t1.dense[31],t1.dense[32],t1.dense[33],t1.dense[34],t1.dense[35],
         |t1.dense[36],t1.dense[37],t1.dense[38],t1.dense[39],t1.dense[40],t1.dense[41],t1.dense[42]) as dense
         |from training_data t1 left join idea_table t2 on t1.dense[11]=t2.ideaidhash where t2.ideaid is null
         |""".stripMargin)

    new_ideaid_data.repartition(1000).select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/newad/$oneday/")
    val CountPathTmpNamev3 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/newad/tmp/"
    val CountPathNamev3 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/newad/$oneday/count"
    val today_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/newad/$oneday/part*")
    CommonUtils.writeCountToFile(spark, today_sample.count(), CountPathTmpNamev3, CountPathNamev3)

    val old_ideaid_data = spark.sql(
      s"""
         |select t1.idx2, t1.idx1, t1.id_arr, t1.idx0, t1.sample_idx, t1.label, t1.dense
         |from training_data t1 left join idea_table t2 on t1.dense[11]=t2.ideaidhash where t2.ideaid is not null
         |""".stripMargin)

    old_ideaid_data.repartition(1000).select($"sample_idx",$"idx0",$"idx1",$"idx2",$"id_arr", $"label", $"dense").write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/oldad/$oneday/")
    val CountPathTmpNamev4 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/oldad/tmp/"
    val CountPathNamev4 = s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/oldad/$oneday/count"
    val old_sample = spark.read.format("tfrecords").option("recordType", "Example").load(s"hdfs://emr-cluster/user/cpc/aiclk_dataflow/daily/cold_start/adlist/oldad/$oneday/part*")
    CommonUtils.writeCountToFile(spark, old_sample.count(), CountPathTmpNamev4, CountPathNamev4)

  }
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + "#" + num, 0) else Murmur3Hash.stringHash64(prefix + "#", 0)
  }
}
