package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.ml.dnn.Utils.CommonUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * author: huazhenhao
  * date: 6/3/19
  */
object DssmTrain {
  Logger.getRootLogger.setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("dssm-train")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val date = args(0)
    val userFeature = getUserFeature(spark, date)
    userFeature.createOrReplaceTempView("userfeature")
    val adFeature = getAdFeature(spark, date)
    adFeature.createOrReplaceTempView("adfeature")
    val samples = getSample(spark, date)
    samples.createOrReplaceTempView("samples")

    println("userFeature count : ",userFeature.count())
    println("adFeature count : ",adFeature.count())
    println("samples count : ",samples.count())

    var result = spark.sql(
      s"""
         |select
         |  a.label as label,
         |  c.u_dense as u_dense,
         |  c.u_idx0 as u_idx0,
         |  c.u_idx1 as u_idx1,
         |  c.u_idx2 as u_idx2,
         |  c.u_id_arr as u_id_arr,
         |  b.ad_dense as ad_dense,
         |  b.ad_idx0 as ad_idx0,
         |  b.ad_idx1 as ad_idx1,
         |  b.ad_idx2 as ad_idx2,
         |  b.ad_id_arr as ad_id_arr
         |from (select * from samples) a
         | left join
         | (select ideaid, unitid, ad_dense, ad_idx0, ad_idx1, ad_idx2, ad_id_arr from adfeature) b
         | on (a.ideaid = b.ideaid and a.unitid = b.unitid)
         | left join
         | (select uid, u_dense, u_idx0, u_idx1, u_idx2, u_id_arr from userfeature) c
         | on (a.uid = c.uid)
       """.stripMargin).rdd.zipWithUniqueId()
      .map {
        x =>
          (
            x._2,
            x._1.getAs[Seq[Int]]("label"),
            x._1.getAs[Seq[Long]]("u_dense"),
            x._1.getAs[Seq[Int]]("u_idx0"),
            x._1.getAs[Seq[Int]]("u_idx1"),
            x._1.getAs[Seq[Int]]("u_idx2"),
            x._1.getAs[Seq[Long]]("u_id_arr"),
            x._1.getAs[Seq[Long]]("ad_dense"),
            x._1.getAs[Seq[Int]]("ad_idx0"),
            x._1.getAs[Seq[Int]]("ad_idx1"),
            x._1.getAs[Seq[Int]]("ad_idx2"),
            x._1.getAs[Seq[Long]]("ad_id_arr")
          )
      }
    println(s"full result: ${result.count()}")
    result = result.filter(_._2 != null)
    println(s"uid is not null: ${result.count()}")
    result = result.filter(_._8 != null)
    println(s"ad is not null: ${result.count()}")

    val train = result.toDF("sample_idx", "label",
      "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr",
      "ad_dense", "ad_idx0", "ad_idx1", "ad_idx2", "ad_id_arr")
    val n = train.count()
    println("所有数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))
    val data = train.randomSplit(Array[Double](0.95, 0.05))
    val trainSample = data(0)
    val testSample = data(1)
    println("train: ", trainSample.count())
    trainSample.repartition(200)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/train-v0/" + date)
    println("test: ", testSample.count())
    testSample.repartition(200)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/test-v0/" + date)


    val traincount=trainSample.count()
    val testcount=testSample.count()


    val trainCountPathTmpName = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/train-v0/tmp/"
    val trainCountPathName = CommonUtils.HDFS_PREFIX_PATH + s"/user/cpc/hzh/dssm/train-v0/${date}/count"
    writeCountToFile(spark, traincount, trainCountPathTmpName, trainCountPathName)

    val testCountPathTmpName = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/test-v0/tmp/"
    val testCountPathName = CommonUtils.HDFS_PREFIX_PATH + s"/user/cpc/hzh/dssm/test-v0/${date}/count"
    writeCountToFile(spark, testcount, testCountPathTmpName, testCountPathName)

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
