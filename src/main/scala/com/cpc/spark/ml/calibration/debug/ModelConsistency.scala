package com.cpc.spark.ml.calibration.debug

import com.cpc.spark.common.Murmur3Hash.stringHash64
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttCvrV3.LogToPb
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object ModelConsistency{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val dt = args(0)
    val modelName = args(1)
    val task = args(2)

    println(s"dt=$dt")
    println(s"modelName=$modelName")

    val dnn_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/wy/dnn_prediction/$task/2019-09-16/result-*")
      .toDF("id","prediction","num")

    println("sum is %d".format(dnn_data.count()))
    // get union log

    val sql = s"""
                 |select a.searchid, a.raw_ctr,b.f84[0] as model_id
                 |from dl_cpc.cpc_basedata_union_events a
                 |join dl_cpc.cpc_ml_nested_snapshot b
                 |  on a.searchid = b.searchid and pt='qtt'
                 |  and b.day = '$dt' and b.hour = '19' and b.f84[0]='ad03717cceb1a53dd917b89ba2fe48e0'
                 |  where a.day = '$dt' and a.hour = '19'
                 |  and a.media_appsid in ('80000001','80000002') and a.isshow = 1
                 |  and a.adsrc = 1 and a.ctr_model_name = 'qtt-list-dnn-rawid-v4-dsp'
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = spark.sql(sql)
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"inner")
      .withColumn("ectr",col("prediction")*1e6d.toInt)
      .withColumn("bias",col("raw_ctr")/col("ectr"))
      .withColumn("flag",when(col("bias")>0.98 and col("bias")<1.02,1).otherwise(0))

    basedata.show(10)
    basedata.filter("flag=0").write.mode("overwrite").saveAsTable("dl_cpc.wy_test02")
    println("sum is %d".format(basedata.count()))
    println("right is %d".format(basedata.filter("flag=1").count()))
    println("bias is %f".format(basedata.groupBy().agg(avg(col("bias")).alias("avg_bias"))
      .first().getAs[Double]("avg_bias")))
    println("model id %d".format(basedata.groupBy("model_id").agg(count(col("id"))).count()))



  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}

}
