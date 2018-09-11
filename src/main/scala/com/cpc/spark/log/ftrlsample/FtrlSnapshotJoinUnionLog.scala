package com.cpc.spark.log.ftrlsample

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._


object FtrlSnapshotJoinUnionLog {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val dt = args(0)
    val hour = args(1)
    val featureColumns = args(2).split(",").toSeq


    val snapshot1 = spark.table("dl_cpc.ml_snapshot_from_show").filter(s"`date` = '$dt' and hour = '$hour'")


    val unionlog1 = spark.table("dl_cpc.cpc_union_log").filter(s"`date` = '$dt' and hour = '$hour'")

    val join = unionlog1.join(snapshot1, Seq("searchid"), "left").filter("feature_vector is not null")

    join.createOrReplaceTempView("joinTable")

    println(s"snapshot1 count = ${snapshot1.count()}")
    println(s"unionlog1 count = ${unionlog1.count()}")
    println(s"join count = ${join.count()}")

    // 抽取结果表
    var sqlRequest =
      s"""
         | select
         |        isclick,
         |        sex,
         |        age,
         |        ext["adclass"].int_value as adclass,
         |        adslot_type,
         |        media_appsid,
         |        adtype,
         |        interaction,
         |        ext["phone_level"].int_value as phone_level,
         |        int(hour) as hour,
         |        os,
         |        isp,
         |        adslotid,
         |        ext["city_level"].int_value as city_level,
         |        ext["pagenum"].int_value as pagenum,
         |        ext["user_req_ad_num"].int_value as user_req_ad_num,
         |        ext["user_req_num"].int_value as user_req_num,
         |        ext["click_count"].int_value as click_count,
         |        ext["click_unit_count"].int_value as click_unit_count,
         |        ext["long_click_count"].int_value as long_click_count,
         |        ctr as base_ctr
         | from
         |        joinTable
         | where
         |        isclick is not null
      """.stripMargin

    val rawData = spark.sql(sqlRequest)
    // 根据列名称抽取数据
    val cleanData = rawData.select(featureColumns.map(c => col(c)): _*)

    // 获取结果RDD
    val resultRDD = getLibSVM(cleanData)

    // 将结果RDD整理成Dataframe准备存储
    val resultDF = resultRDD.toDF("libsvm", "isclick", "adslot_type", "media_appsid")
    val result = resultDF.withColumn("date", lit(dt)).withColumn("hour", lit(hour))

    // 存取dataframe
    // TODO：数据表名暂不确定
    result.write.mode("append").partitionBy("date", "hour").saveAsTable("test.tmp_libsvm_table_20180911")

  }

  def getLibSVM(df: DataFrame) = {
    // 读取原始数据表并进行结构转化

    // 获取dataframe列名
    val featureCols = df.columns

    // 转换各列数据类型并填补缺失值
    var finalData = df
    for (currentFeature <- featureCols) {
      finalData = finalData.withColumn(currentFeature, col(currentFeature).cast(types.StringType))
    }
    val finalDF = finalData.na.fill("null")

    // 生成特征数据
    val feature = finalDF.map( row => {
      val currentRow =
        for (index <- (1 to featureCols.size - 1))
          yield {
            val tmpIndex = index - 1
            if (row(index) != "null") tmpIndex.toString + ":" + row.getString(index)
            else ""
          }
      currentRow.reduce((x, y) => x + " " + y).replace("  ", " ")
    }
    ).rdd

    // 生成标签数据
    val label = finalDF.select(col("isclick")).map(_.getString(0)).rdd

    // 生成adslot_type列
    val adslotType = finalDF.select(col("adslot_type")).map(_.getString(0)).rdd

    // 生成libsvm格式列
    val libsvmResult = label zip feature map { case(x, y) =>
      x + " " + y
    }
    // 生成media_appsid格式列
    val mediaAppsid = finalDF.select(col("media_appsid")).map(_.getString(0)).rdd

    val resultRDD = libsvmResult zip label zip adslotType zip mediaAppsid map {case(((x, y), z), a) => (x, y, z, a)}

    resultRDD

  }

}

