package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.Utils.CommonUtils
import com.cpc.spark.ml.dnn.retrieval.DssmRetrieval._
import com.cpc.spark.ml.dnn.retrieval.DssmUserGen.sparseVector
import com.qtt.aiclk.featurestore.Feaconf.FeatureStore
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
  * author: huazhenhao
  * date: 11/13/18
  */
object DssmAdGen {
  Logger.getRootLogger.setLevel(Level.WARN)

  val ad_day_feature_list: Seq[String] = Seq(
    "ad_title_token"
  )

  val ad_day_feature_map: Map[String, Int] = Map[String, Int] (
    "ad_title_token" -> 0
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dssm-ad-gen")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)

    val adInfo = getData(spark, date)

    val adCount=adInfo.count()

    println(s"Ad Info：total = ${adCount}")

    adInfo.repartition(10)
      .write
      .mode("overwrite")
      .parquet(CommonUtils.HDFS_PREFIX_PATH +"/user/cpc/hzh/dssm/ad-info-v0-debug/" + date)

    adInfo.repartition(10)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(CommonUtils.HDFS_PREFIX_PATH +"/user/cpc/hzh/dssm/ad-info-v0/" + date)

    val adCountPathTmpName = CommonUtils.HDFS_PREFIX_PATH + "/user/cpc/hzh/dssm/ad-info-v0/tmp/"
    val adCountPathName = CommonUtils.HDFS_PREFIX_PATH + s"/user/cpc/hzh/dssm/ad-info-v0/${date}/count"
    CommonUtils.writeCountToFile(spark, adCount, adCountPathTmpName, adCountPathName)
  }

  def getAdOneHotFeatures(spark: SparkSession, date: String): RDD[(String, (String, Array[Long]))] = {
    import spark.implicits._
    val sql =
      s"""
         |select
         |  cast(ideaid as string) as ideaid,
         |  cast(unitid as string) as unitid,
         |  max(adtype) as adtype,
         |  max(interaction) as interaction,
         |  max(bid) as bid,
         |  max(planid) as planid,
         |  max(userid) as userid,
         |  max(adclass) as adclass,
         |  max(siteid) as site_id,
         |  max(ad_title) as ad_title
         |from dl_cpc.cpc_basedata_union_events where `day` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and length(uid) > 1
         |group by ideaid, unitid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    spark.sql(sql).select(
      $"ideaid",
      $"unitid",

      hash("a1")($"ideaid").alias("a1"),
      hash("a2")($"unitid").alias("a2"),
      hash("a3")($"adtype").alias("a3"),
      hash("a4")($"interaction").alias("a4"),
      hash("a5")($"bid").alias("a5"),
      hash("a6")($"planid").alias("a6"),
      hash("a7")($"userid").alias("a7"),
      hash("a8")($"adclass").alias("a8"),
      hash("a9")($"site_id").alias("a9")

    ).rdd.map(row => {
      val ideaID = row.getAs[String]("ideaid")
      val unitID = row.getAs[String]("unitid")

      val denseArray = new Array[Long](9)
      for (i <- 0 until 9) {
        denseArray(i) = row.getAs[Long]("a" + (i + 1).toString)
      }
      (ideaID, (unitID, denseArray))
    })
  }

  def getAdMultiHotFeatures(spark: SparkSession, date: String): RDD[(String, Array[Array[Long]])] = {
    val sql =
      s"""
         |select cast(ideaid as string) as ideaid, content from dl_cpc.ad_day_feature
         |where dt = '$date' and (pt = 'merge')
       """.stripMargin
    println(sql)
    val df = spark.sql(sql)
    println("ad df count: " + df.count())
    println(ad_day_feature_map)
    val result = df.rdd.groupBy(row => row.getAs[String]("ideaid")).flatMap(x => {
      var featureList = new ListBuffer[(String, Int, Seq[String])]()
      val uid = x._1
      x._2.foreach(row => {
        val fs = FeatureStore.newBuilder().mergeFrom(Base64.decodeBase64(row.getAs[String]("content")))
        for (feature <- fs.getFeaturesList) {
          val name = feature.getName
          if (ad_day_feature_map.contains(name)) {
            val featureType = feature.getType
            val featureValue = mutable.ArrayBuffer[String]()
            // str: 1/ int: 2 / float: 3
            featureType match {
              case 1 => if (feature.getStrListCount > 0) {
                for (fv <- feature.getStrListList) {
                  featureValue.add(fv)
                }
              }
              case 2 => if (feature.getIntListCount > 0) {
                for (fv <- feature.getIntListList) {
                  featureValue.add(fv.toString)
                }
              }
              case 3 => if (feature.getFloatListCount > 0) {
                for (fv <- feature.getFloatListList) {
                  featureValue.add(fv.toString)
                }
              }
            }
            featureList += ((uid, ad_day_feature_map.getOrElse(name, 0), featureValue))
          }
        }
      })
      featureList.iterator
    }).groupBy(_._1).map(x => {
      val uid = x._1
      val multiArray = new Array[Array[Long]](ad_day_feature_list.size)
      x._2.foreach(entry => {
        val index = entry._2
        val values = entry._3.slice(0, 1000)
        multiArray(index) = new Array[Long](values.size)
        for (i <- values.indices) {
          multiArray(index)(i) = Murmur3Hash.stringHash64("a" + index.toString + values.get(i), 0)
        }
      })
      (uid, multiArray)
    })
    result
  }

  def getData(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val adOneHotFeatures = getAdOneHotFeatures(spark, date)
    println("ad onehot size: " + adOneHotFeatures.count())
    val adMultiHotFeatures = getAdMultiHotFeatures(spark, date)
    println("ad multihot size: " + adMultiHotFeatures.count())

    val multiHotCounter = spark.sparkContext.longAccumulator("multiHotCounter")
    val featureCounters = new Array[LongAccumulator](ad_day_feature_list.length)
    for (i <- featureCounters.indices) {
      featureCounters(i) = spark.sparkContext.longAccumulator("ad_counter_" + ad_day_feature_list(i))
    }
    val result = adOneHotFeatures.leftOuterJoin(adMultiHotFeatures).map(x => {
      val ideaID: String = x._1
      val unitID: String = x._2._1._1
      val dense: Seq[Long] = x._2._1._2.toSeq
      if (x._2._2.orNull != null) {
        multiHotCounter.add(1)
      }
      val sparseResult = sparseVector(x._2._2.orNull, featureCounters, ad_day_feature_list, "a")
      (ideaID, unitID, dense, sparseResult)
    }).zipWithUniqueId()
      .map { x =>
        (x._2,
          x._1._1 + '_' + x._1._2,
          x._1._3,
          x._1._4._1,
          x._1._4._2,
          x._1._4._3,
          x._1._4._4
        )
      }
      .toDF("sample_idx", "adid", "ad_dense", "ad_idx0", "ad_idx1", "ad_idx2", "ad_id_arr")
    println("result joined size: " + result.count())
    println("ad day match count: " + multiHotCounter.value)
    for (i <- featureCounters.indices) {
      println(s"${ad_day_feature_list(i)} match counts: ${featureCounters(i).value}")
    }
    result
  }
}
