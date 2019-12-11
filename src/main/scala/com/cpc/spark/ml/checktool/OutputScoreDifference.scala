package com.cpc.spark.ml.checktool

import com.cpc.spark.common.Murmur3Hash.stringHash64
import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQttCvrV3.LogToPb
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

/**
  * author: wangyao
  * date: 12/14/19
  */
object OutputScoreDifference{
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    val dt = args(0)
    val modelName = args(1)
    val task = args(2)

    println(s"dt=$dt")
    println(s"modelName=$modelName")

    val dnn_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/wy/dnn_model_score_offline/$task/$dt/result-*")
      .toDF("id","prediction","num")

    dnn_data.show(10)

    println("sum is %d".format(dnn_data.count()))
    // get union log

    val sql = s"""
                 |select * from dl_cpc.dnn_model_score_online where day ='$dt' and model ='$modelName'
       """.stripMargin
    println(s"sql:\n$sql")
    val basedata = spark.sql(sql)
      .withColumn("id",hash64(0)(col("searchid")))
      .join(dnn_data,Seq("id"),"inner")
      .selectExpr("raw","cast(prediction*1e6d as Int) p_offline")
      .withColumn("diff",col("p_offline")/col("raw"))
      .withColumn("diff",restrict(col("diff")))

    basedata.show(10)
    val sum = basedata.count()
    println("sum is %d".format(sum))
   val result = basedata
       .withColumn("diff",when(col("diff")<0.8,0.8).otherwise(col("diff")))
       .withColumn("diff",when(col("diff")>1.2,1.2).otherwise(col("diff")))
       .groupBy("diff").count()
       .orderBy("diff")
       .withColumn("ratio",col("count")/sum)

    result.show(100)


  }

  def hash64(seed:Int)= udf {
    x:String =>  stringHash64(x,seed)}


  def restrict= udf {
    x:Double => (x*100).toInt.toDouble/100}

}
