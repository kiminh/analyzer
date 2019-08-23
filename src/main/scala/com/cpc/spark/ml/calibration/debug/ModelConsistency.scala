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

    val dnn_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/wy/dnn_prediction/$task/result-*")
      .toDF("id","prediction","num")

    println("sum is %d".format(dnn_data.count()))
    // get union log

    val sql = s"""
                 |select searchid, raw_ctr
                 |from dl_cpc.cpc_basedata_union_events
                 |  where day ='$dt' and hour='12'
                 |  and media_appsid in ('80000001','80000002') and isshow = 1
                 |  and adsrc = 1 and ctr_model_name = 'qtt-list-dnn-rawid-v4-dsp'
                 |  AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = spark.sql(sql)
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"inner")
      .withColumn("ectr",col("prediction")*1e6d.toInt)
      .withColumn("flag",when(col("ectr")/col("raw_ctr")>0.99 and col("ectr")/col("raw_ctr")<1.01,1).otherwise(0))

    basedata.show(10)
    println("sum is %d".format(basedata.count()))
    println("right is %d".format(basedata.filter("flag=1").count()))


  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}

}
