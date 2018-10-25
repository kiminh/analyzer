package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession


object OcpcTestFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val df = readInnocence(spark)

  }


  def readInnocence(spark: SparkSession) ={
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_ideaid.txt"
    val data = spark.sparkContext.textFile(filename)

    val dataRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    //    dataRDD.foreach(println)

    dataRDD.toDF("ideaid", "flag").createOrReplaceTempView("innocence_list")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  flag
         |FROM
         |  innocence_list
         |GROUP BY ideaid, flag
       """.stripMargin

    val resultDF = spark.sql(sqlRequest)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_innocence_idea_list")
    resultDF
  }
}
