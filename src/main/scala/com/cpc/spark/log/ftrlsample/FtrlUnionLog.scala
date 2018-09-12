package com.cpc.spark.log.ftrlsample

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, types}
import org.apache.spark.sql.functions._
import scala.collection.mutable

object FtrlUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val dt = args(0)
    val hour = args(1)
    val featureColumns = args(2).split(",").toSeq
    val tabName = "test.tmp_libsvm_unionLog_table_" + dt + "_" + hour

    ftrlUnionLog(dt, hour, featureColumns, tabName, spark)
  }

  def ftrlUnionLog(date: String, hour:String, featureColumns: Seq[String], tableName: String, spark: SparkSession) ={
    import spark.implicits._

    var sqlRequest =
      s"""
         | select
         |        a.searchid,
         |        isclick,
         |        label,
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
         |      (
         |        select *
         |        from dl_cpc.cpc_union_log
         |        where isclick is not null and
         |        isshow = 1 and ext['antispam'].int_value = 0 and ideaid > 0
         |        and `date` = '$date' and hour = '$hour'
         |      ) a
         |      left outer join
         |      (
         |        select searchid, label
         |        from dl_cpc.ml_cvr_feature_v1
         |        where `date` = '$date' and label is not null
         |      ) b on a.searchid = b.searchid
    """.stripMargin
    val unionLog = spark.sql(sqlRequest)
    val cleanData = unionLog.select(featureColumns.map(c => col(c)): _*)
    // 转换各列数据类型并填补缺失值
    var finalData = cleanData
    for (currentFeature <- featureColumns) {
      finalData = finalData.withColumn(currentFeature, col(currentFeature).cast(types.StringType))
    }
    val finalDF = finalData.na.fill("null")

    var featureMap = mutable.LinkedHashMap[String, String]()
    featureMap += ("sex" -> "50") //bs
    featureMap += ("age" -> "51") //bs
    featureMap += ("adclass" -> "52") //bs
    featureMap += ("adslot_type" -> "53") //bs
    featureMap += ("media_appsid" -> "54") //bs
    featureMap += ("adtype" -> "55") //bs
    featureMap += ("interaction" -> "56") //bs
    featureMap += ("phone_level" -> "57") //bs
    featureMap += ("hour" -> "58") //bs
    featureMap += ("os" -> "59") //bs
    featureMap += ("isp" -> "60")
    featureMap += ("adslotid" -> "61") //bs
    featureMap += ("city_level" -> "62") //bs
    featureMap += ("pagenum" -> "63")
    featureMap += ("user_req_ad_num" -> "64") //bs
    featureMap += ("user_req_num" -> "65") //bs
    featureMap += ("click_count" -> "66") //bs
    featureMap += ("click_unit_count" -> "67") //bs
    featureMap += ("long_click_count" -> "68") //bs
    //    featureMap += ("week" -> "69")
    featureMap += ("base_ctr" -> "70") //bs

    // 生成特征数据
    val feature = finalDF.map( row => {
      val currentRow =
        for (index <- featureColumns.indices)
          yield {
            val tmpIndex = featureMap.getOrElse(featureColumns(index), "null")
            if (row(index) != "null" & tmpIndex != "null") tmpIndex.toString + ":" + row.getString(index)
            else ""
          }
      currentRow.reduce((x, y) => x + " " + y).replace("  ", " ")
    }
    ).rdd

    // 生成标签数据
    val isClick = finalDF.select(col("isclick")).map(_.getString(0)).rdd
    val label = finalDF.select(col("label")).map(_.getString(0)).rdd
    val searchId = finalDF.select(col("searchid")).map(_.getString(0)).rdd
    // 生成adslot_type列
    val adslotType = finalDF.select(col("adslot_type")).map(_.getString(0)).rdd

    // 生成media_appsid格式列
    val mediaAppsid = finalDF.select(col("media_appsid")).map(_.getString(0)).rdd

    val resultRDD = searchId zip feature zip isClick zip label zip adslotType zip mediaAppsid map {case(((((x, y), z), a), b), c) => (x, y, z, a, b, c)}
//    println(resultRDD.first)
    // 将结果RDD整理成Dataframe准备存储
    val resultDF = resultRDD.toDF("searchid", "libsvm", "isclick", "iscvr", "adslot_type", "media_appsid")
    val result = resultDF.withColumn("date", lit(date)).withColumn("hour", lit(hour))

    // 存取dataframe
    // TODO：数据表名暂不确定
    result.write.mode("overwrite").saveAsTable(tableName)

    println("complete unionLog Function")
  }

}


//real    6m22.437s
//user    1m25.140s
//sys     0m11.149s
