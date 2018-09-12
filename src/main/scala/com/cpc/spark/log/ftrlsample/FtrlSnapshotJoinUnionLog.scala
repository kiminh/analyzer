package com.cpc.spark.log.ftrlsample

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._


object FtrlSnapshotJoinUnionLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


    val dt = args(0)
    val hour = args(1)
    val featureColumns = args(2).split(",").toSeq

    ftrlJoinTable(dt, hour, "test.tmp_libsvm_table_20180912", spark)
  }


  def ftrlJoinTable(date: String, hour: String, targetTable: String, spark: SparkSession) = {
    import spark.implicits._

    val snapshot1 = spark.table("dl_cpc.ml_snapshot_from_show").filter(s"`date` = '$date' and hour = '$hour'")

    val unionlog1 = spark.table("test.tmp_libsvm_unionLog_table_20180911").filter(s"`date` = '$date' and hour = '$hour'")

    val join = unionlog1.join(snapshot1, Seq("searchid"), "inner").filter("feature_vector is not null")

    val rawData = join.select(col("feature_vector"), col("libsvm"))

    val finalLibSvm = rawData.map(row => {
      val featureVector = row.getMap(0)
      val featureVectorString = featureVector.reduce((k, v) => k.toString() + ":" + v.toString())
      val libsvm = row.getString(1)
      val currentRow = featureVector + " " + libsvm
      val currentResult = currentRow.replace("  ", " ")
      currentResult
    }).rdd

    val isClick = rawData.select(col("isclick")).map(_.getString(0)).rdd
    val label = rawData.select(col("label")).map(_.getString(0)).rdd
    // 生成adslot_type列
    val adslotType = rawData.select(col("adslot_type")).map(_.getString(0)).rdd

    // 生成media_appsid格式列
    val mediaAppsid = rawData.select(col("media_appsid")).map(_.getString(0)).rdd

    val resultRDD = finalLibSvm zip isClick zip label zip adslotType zip mediaAppsid map { case ((((x, y), z), a), b) => (x, y, z, a, b) }
    println(resultRDD.first)

    // 存取dataframe
    // TODO：数据表名暂不确定
    //    result.write.mode("overwrite").partitionBy("date", "hour").saveAsTable(targetTable)

    println("complete unionLog Function")
  }

}


//TODO:
// 1. seperate unionlog and snapshot in two different steps and save them in table separately
// Some -> integer
// Null value
// 2. use udf
// udf(Array[String])
// val array  = features
// udf => for (feature <- features) { if }