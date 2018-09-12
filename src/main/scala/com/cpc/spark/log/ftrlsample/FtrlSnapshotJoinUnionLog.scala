package com.cpc.spark.log.ftrlsample

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._


object FtrlSnapshotJoinUnionLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


    val dt = args(0)
    val hour = args(1)

    ftrlJoinTable(dt, hour, "test.tmp_libsvm_table_20180912", spark)
  }

  def ftrlJoinTable(date: String, hour: String, targetTable: String, spark: SparkSession) = {
    import spark.implicits._

    val snapshot1 = spark.table("dl_cpc.ml_snapshot_from_show").filter(s"`date` = '$date' and hour = '$hour'")

    val unionlog1 = spark.table("test.tmp_libsvm_unionLog_table_20180912").filter(s"`date` = '$date' and hour = '$hour'")

    val join = unionlog1.join(snapshot1, Seq("searchid"), "inner").filter("feature_vector is not null")

//    featureVector: org.apache.spark.sql.DataFrame = [feature_vector: map<int,float>]
    val featureVectorRDD = join.select(col("feature_vector")).rdd
    val featureVector = featureVectorRDD.map( row =>
      {row.getMap[Int, Float](0).map(_.productIterator.mkString(":"))}.mkString(" ").trim
    )
    val unilogFeature = join.select(col("libsvm")).map(_.getString(0).trim).rdd

    val finalLibSvm = unilogFeature zip featureVector map { case(x, y) =>
      x + " " + y
    }

    val isClick = join.select(col("isclick")).map(_.getString(0)).rdd
    val label = join.select(col("iscvr")).map(_.getString(0)).rdd
    // 生成adslot_type列
    val adslotType = join.select(col("adslot_type")).map(_.getString(0)).rdd

    // 生成media_appsid格式列
    val mediaAppsid = join.select(col("media_appsid")).map(_.getString(0)).rdd

    val resultRDD = finalLibSvm zip isClick zip label zip adslotType zip mediaAppsid map { case ((((x, y), z), a), b) => (x, y, z, a, b) }
    println(resultRDD.first)

    val resultDF = resultRDD.toDF("libsvm", "isclick", "iscvr", "adslot_type", "media_appsid")
    val result = resultDF.withColumn("date", lit(date)).withColumn("hour", lit(hour))
    // 存取dataframe
    // TODO：数据表名暂不确定
    result.write.mode("overwrite").partitionBy("date", "hour").saveAsTable(targetTable)

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