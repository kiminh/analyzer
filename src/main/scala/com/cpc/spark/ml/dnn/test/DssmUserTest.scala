package com.cpc.spark.ml.dnn.test

import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * author: huazhenhao
  * date: 6/3/19
  */
object DssmUserTest {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DssmUserTest")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val date = args(0)
    val userFeature = getUserFeature(spark, date)
    userFeature.createOrReplaceTempView("userfeature")

    var result = spark.sql(
      s"""
         |select uid, u_dense, u_idx0, u_idx1, u_idx2, u_id_arr from userfeature where u_dense is null
       """.stripMargin)

    println("result count = " + result.count())

    result.show(10)

  }

  private def writeCountToFile(spark: SparkSession, count: Long, trainCountPathTmpName: String, trainCountPathName: String) = {
    val arr = Array(count)
    val rdd = spark.sparkContext.parallelize(arr).repartition(1)

    CommonUtils.rddWriteFile(spark, trainCountPathTmpName, trainCountPathName, rdd)
  }

  def getUserFeature(spark: SparkSession, date: String): DataFrame = {
    spark.read.parquet(CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/all-user-info/" + date)
  }

  def getAdFeature(spark: SparkSession, date: String): DataFrame = {
    spark.read.parquet(CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/ad-info-v0-debug/" + date)
  }

  def getSample(spark: SparkSession, date: String): DataFrame = {
    val sql =
      s"""
         |select if(isclick>0, array(1,0), array(0,1)) as label,
         | uid, ideaid, unitid
         |from dl_cpc.cpc_basedata_union_events where day = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002") -- 趣头条媒体
         |  and adsrc=1 -- 来自CPC
         |  and ideaid>0 -- 来自CPC
         |  and adslot_type=1 -- 列表页
         |  and isshow=1 -- 有曝光
         |  and uid not like "%.%" -- 去除无效uid
         |  and uid not like "%000000%" -- 去除无效uid
         |  and length(uid) in (14, 15, 36) -- 去除无效uid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")
    spark.sql(sql)
  }
}
