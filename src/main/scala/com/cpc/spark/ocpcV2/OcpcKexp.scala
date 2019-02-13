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
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc." + tableName)
    println(s"save data into table: $tableName")
  }

  def selectKbyEXP(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val selectCondition = s"`date`='$date' and `hour`='$hour'"

    val ocpcConversionGoal = spark.table("test.ocpc_idea_update_time_" + hour)
    val kv1Raw = spark
      .table("dl_cpc.ocpc_v2_k_new")
      .where(selectCondition)
      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3")

    val kv1 = kv1Raw
      .join(ocpcConversionGoal, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3", "conversion_goal")
      .withColumn("k2", when(col("conversion_goal").isNotNull && col("conversion_goal")===3, col("k_ratio3")).otherwise(col("k_ratio1")))
      .withColumn("k3", col("k_ratio2"))
      .withColumn("k_ratio2_v1", col("k2"))
      .withColumn("k_ratio3_v1", col("k3"))
      .select("ideaid", "k_ratio2_v1", "k_ratio3_v1")

    val kv2 = spark
      .table("dl_cpc.ocpc_regression_k")
      .where(selectCondition)
      .withColumn("k_ratio2_v2", col("k_ratio2"))
      .withColumn("k_ratio3_v2", col("k_ratio3"))
      .select("ideaid", "k_ratio2_v2", "k_ratio3_v2")

    val kv3 = spark
      .table("dl_cpc.ocpc_v2_app_open_k")
      .where(selectCondition)
      .withColumn("k_ratio2_v3", col("k_ratio2"))
      .withColumn("k_ratio3_v3", col("k_ratio3"))
      .select("ideaid", "k_ratio2_v3", "k_ratio3_v3")
    
    val kv4Raw = spark
      .table("dl_cpc.ocpc_pcoc_k_hourly")
      .where(selectCondition)
      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3")

    val kv4 = kv4Raw
      .join(ocpcConversionGoal, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio1", "k_ratio2", "k_ratio3", "conversion_goal")
      .withColumn("k2", when(col("conversion_goal").isNotNull && col("conversion_goal")===3, col("k_ratio3")).otherwise(col("k_ratio1")))
      .withColumn("k3", col("k_ratio2"))
      .withColumn("k_ratio2_v4", col("k2"))
      .withColumn("k_ratio3_v4", col("k3"))
      .select("ideaid", "k_ratio2_v4", "k_ratio3_v4")

    // 读取实验ideaid列表
    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val expIdeas = rawRDD
      .toDF("ideaid", "flag")
      .groupBy("ideaid")
      .agg(min(col("flag")).alias("flag"))
      .distinct()

    // 根据实验id列表，替换k值
    // todo
    val kvalue = kv1
      .join(kv2, Seq("ideaid"), "outer")
      .join(kv3, Seq("ideaid"), "outer")
      .join(kv4, Seq("ideaid"), "outer")
      .join(expIdeas, Seq("ideaid"), "left_outer")
      .select("ideaid", "k_ratio2_v1", "k_ratio3_v1", "k_ratio2_v2", "k_ratio3_v2", "k_ratio2_v3", "k_ratio3_v3", "k_ratio2_v4", "k_ratio3_v4", "flag")
      .withColumn("k_ratio2", col("k_ratio2_v1"))
      .withColumn("k_ratio3", col("k_ratio3_v1"))
      .withColumn("k_ratio2", when(col("flag") === 1, col("k_ratio2_v2")).otherwise(col("k_ratio2")))
      .withColumn("k_ratio3", when(col("flag") === 1, col("k_ratio3_v2")).otherwise(col("k_ratio3")))
      .withColumn("k_ratio2", when(col("flag") === 2, col("k_ratio2_v3")).otherwise(col("k_ratio2")))
      .withColumn("k_ratio3", when(col("flag") === 2, col("k_ratio3_v3")).otherwise(col("k_ratio3")))
      .withColumn("k_ratio2", when(col("flag") === 3, col("k_ratio2_v4")).otherwise(col("k_ratio2")))
      .withColumn("k_ratio3", when(col("flag") === 3, col("k_ratio3_v4")).otherwise(col("k_ratio3")))
//      .withColumn("k_ratio2", when(col("flag") === 1 && col("conversion_goal") < 3, col("k_ratio2_v2")).otherwise(col("k_ratio2_v1")))
//      .withColumn("k_ratio3", when(col("flag") === 1, col("k_ratio3_v2")).otherwise(col("k_ratio3_v1")))
//      .withColumn("k_ratio2", when(col("flag") === 2 && col("conversion_goal") === 1, col("k_ratio2_v3")).otherwise(col("k_ratio2")))




//    test.ocpc_k_exp_middle_hourly
    kvalue
      .select("ideaid", "k_ratio2_v1", "k_ratio3_v1", "k_ratio2_v2", "k_ratio3_v2", "k_ratio2_v3", "k_ratio3_v3", "k_ratio2_v4", "k_ratio3_v4", "flag", "k_ratio2", "k_ratio3")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(10)
      .write
      .mode("overwrite")
      .saveAsTable("test.ocpc_k_exp_middle_hourly20190212")
//      .insertInto("dl_cpc.ocpc_k_exp_middle_hourly")


    kvalue.show(10)
    val resultDF = kvalue
      .select("ideaid", "k_ratio2", "k_ratio3")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }
}