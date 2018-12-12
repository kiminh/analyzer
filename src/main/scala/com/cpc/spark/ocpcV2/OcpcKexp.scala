package com.cpc.spark.ocpcV2

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object OcpcKexp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val result = selectKbyEXP(date, hour, spark)
    val tableName = "ocpc_regression_k_final"
//    result.write.mode("overwrite").saveAsTable("test." + tableName)
    result.write.mode("overwrite").insertInto("dl_cpc." + tableName)
    println(s"save data into table: $tableName")
  }

  def selectKbyEXP(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val selectCondition = s"`date`='$date' and `hour`='$hour'"
    val kv1 = spark
      .table("dl_cpc.ocpc_v2_k")
      .where(selectCondition)
      .withColumn("k_ratio2_v1", col("k_ratio2"))
      .withColumn("k_ratio3_v1", col("k_ratio3"))
      .select("ideaid", "k_ratio2_v1", "k_ratio3_v1")

    val kv2 = spark
      .table("dl_cpc.ocpc_regression_k")
      .where(selectCondition)
      .withColumn("k_ratio2_v2", col("k_ratio2"))
      .withColumn("k_ratio3_v2", col("k_ratio3"))
      .select("ideaid", "k_ratio2_v2", "k_ratio3_v2")

    // 读取实验ideaid列表
    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val expIdeas = rawRDD.toDF("ideaid", "flag").distinct()

    // 根据实验id列表，替换k值
    val kvalue = kv1
      .join(kv2, Seq("ideaid"), "outer")
      .join(expIdeas, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio2_v1", "k_ratio3_v1", "k_ratio2_v2", "k_ratio3_v2", "flag")
      .withColumn("k_ratio2", when(col("flag") === 1, col("k_ratio2_v2")).otherwise(col("k_ratio2_v1")))
      .withColumn("k_ratio3", when(col("flag") === 1, col("k_ratio3_v2")).otherwise(col("k_ratio3_v1")))

    kvalue
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_k_exp_middle_hourly")


    kvalue.show(10)
    val resultDF = kvalue
      .select("ideaid", "k_ratio2", "k_ratio3")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }
}