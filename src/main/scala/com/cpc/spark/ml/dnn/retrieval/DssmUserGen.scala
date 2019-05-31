package com.cpc.spark.ml.dnn.retrieval

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.retrieval.DssmRetrieval._
import com.qtt.aiclk.featurestore.Feaconf.FeatureStore
import org.apache.commons.codec.binary.Base64
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import scala.collection.mutable

object DssmUserGen {
  Logger.getRootLogger.setLevel(Level.WARN)

  val user_day_feature_list: Seq[String] = Seq(
    "app",
    "s_list_ideaid_1",
    "s_list_ideaid_3",
    "s_list_adclass_1",
    "s_list_adclass_2",
    "s_list_adclass_3",
    "c_list_ideaid_1",
    "c_list_ideaid_2",
    "c_list_ideaid_3",
    "c_list_adclass_1",
    "c_list_adclass_2",
    "c_list_adclass_3",
    "c_list_ideaid_4_7",
    "c_list_adclass_4_7"
  )
  var user_day_feature_map: mutable.Map[String, Int] = mutable.Map[String, Int]()

  def set_user_day_map(): Unit = {
    var index = 0
    for (ele <- user_day_feature_list) {
      user_day_feature_map += (ele -> index)
      index += 1
    }
  }

  def main(args: Array[String]): Unit = {
    set_user_day_map()

    val spark = SparkSession.builder()
      .appName("dssm-user-gen")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val firstTime = args(1).toBoolean

    val userInfo = getData(spark, date)

    println("DAU User count = %d".format(userInfo.count()))

    val finalOutput = if (!firstTime) {
      import spark.implicits._

      val keyedUser = userInfo.rdd.map(x => (x.getAs[String]("uid"), x))

      val allUserInfo = spark.read.parquet("/user/cpc/hzh/dssm/all-user-info")
      allUserInfo.rdd.map(x => (x.getAs[String]("uid"), x))
        .cogroup(keyedUser)
        .map {
          x => {
            val row =
              if (x._2._2 != null && x._2._2.iterator != null && x._2._2.iterator.hasNext) {
                x._2._2.iterator.next()
              } else if (x._2._1 != null && x._2._1.iterator != null && x._2._1.iterator.hasNext) {
                x._2._1.iterator.next()
              } else {
                null
              }
            if (row != null) {
              (row.getAs[Number]("sample_idx").longValue(),
                row.getAs[String]("uid"),
                row.getAs[Seq[Long]]("u_dense"),
                row.getAs[Seq[Int]]("u_idx0"),
                row.getAs[Seq[Int]]("u_idx1"),
                row.getAs[Seq[Int]]("u_idx2"),
                row.getAs[Seq[Long]]("u_id_arr"))
            } else {
              (-1L, "", Seq(0L), Seq(0), Seq(0), Seq(0), Seq(0L))
            }
          }
        }
        .toDF("sample_idx", "uid",
          "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr")
        .filter(row => row.getAs[Long]("sample_idx") > 0)
    } else {
      userInfo
    }

    val n = finalOutput.count()
    println("Final user count = %d".format(n))

    finalOutput.repartition(100)
      .write
      .mode("overwrite")
      .parquet("/user/cpc/hzh/dssm/all-user-info")

    finalOutput.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/hzh/dssm/user-info-v0/" + date)
  }

  def getUserDayFeatures(spark: SparkSession, date: String): RDD[(String, Array[Array[Long]])] = {
    spark.sql(
      s"""
         |select uid, content from dl_cpc.user_day_feature where dt = '$date' and (pt = 'merge' or pt = 'app')
       """.stripMargin
    ).rdd.groupBy(row => row.getAs[String]("uid")).flatMap(x => {
      var featureList = new ListBuffer[(String, Int, Seq[String])]()
      val uid = x._1
      x._2.foreach(row => {
        val fs = FeatureStore.newBuilder().mergeFrom(Base64.decodeBase64(row.getAs[String]("content")))
        for (feature <- fs.getFeaturesList) {
          val name = feature.getName
          if (user_day_feature_map.contains(name)) {
            val featureType = feature.getType
            var featureValue = Seq[String]()
            // str: 1/ int: 2 / float: 3
            featureType match {
              case 1 => if (feature.getStrListCount > 0) {
                featureValue = feature.getStrListList
              }
              case 2 => if (feature.getIntListCount > 0) {
                featureValue = feature.getIntListList.map(_.toString)
              }
              case 3 => if (feature.getFloatListCount > 0) {
                featureValue = feature.getFloatListList.map(_.toString)
              }
            }
            featureList += ((uid, user_day_feature_map.getOrElse(name, -1), featureValue))
          }
        }
      })
      featureList.iterator
    }).groupBy(_._1).map(x => {
      val uid = x._1
      val multiArray = new Array[Array[Long]](user_day_feature_list.size)
      x._2.foreach(entry => {
        val index = entry._2
        multiArray(index) = new Array[Long](entry._3.size)
        for (i <- entry._3.indices) {
          multiArray(index)(i) = Murmur3Hash.stringHash64("u" + index.toString + entry._3.get(i), 0)
        }
      })
      (uid, multiArray)
    })
  }

  def getUserLogFeatures(spark: SparkSession, date: String): RDD[(String, Array[Long])] = {
    import spark.implicits._
    // get user fields from base event
    val sql =
      s"""
         |select
         |  uid,
         |  cast(max(os) as string) as os,
         |  cast(max(network) as string) as network,
         |  cast(max(phone_price) as string) as phone_price,
         |  cast(max(brand_title) as string) as brand,
         |  cast(max(province) as string) as province,
         |  cast(max(city) as string) as city,
         |  cast(max(city_level) as string) as city_level,
         |  cast(max(age) as string) as age,
         |  cast(max(sex) as string) as sex
         | from dl_cpc.cpc_basedata_union_events
         |  where day = '$date'
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%" -- 去除无效uid
         |  and uid not like "%000000%" -- 去除无效uid
         |  and length(uid) in (14, 15, 36) -- 去除无效uid
         |group by uid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    spark.sql(sql).select(
      // user index
      $"uid",
      hash("u1")($"os").alias("u1"),
      hash("u2")($"network").alias("u2"),
      hash("u3")($"phone_price").alias("u3"),
      hash("u4")($"brand").alias("u4"),
      hash("u5")($"province").alias("u5"),
      hash("u6")($"city").alias("u6"),
      hash("u7")($"city_level").alias("u7"),
      hash("u8")($"age").alias("u8"),
      hash("u9")($"sex").alias("u9")
    ).rdd.map(row => {
      val uid = row.getAs[String]("uid")
      val denseArray = new Array[Long](9)
      for (i <- 0 until 9) {
        denseArray(i) = row.getAs[Long]("u" + (i + 1).toString)
      }
      (uid, denseArray)
    })
  }

  // transform to spark vector format
  def sparseVector(value: Array[Array[Long]]): (Seq[Int], Seq[Int], Seq[Int], Seq[Long]) = {
    var i = 0
    var re = Seq[(Int, Int, Long)]()
    // add default hash when value is null
    if (value == null || value.length == 0) {
      for (i <- user_day_feature_list.indices) {
        re ++= Seq(i, 0, Murmur3Hash.stringHash64("u" + i.toString, 0))
      }
    } else {
      for (feature <- value) {
        if (feature != null && feature.length > 0) {
          re ++= feature.zipWithIndex.map(x => (i, x._2, x._1))
        } else {
          re ++= Seq((i, 0, Murmur3Hash.stringHash64("u" + i.toString, 0)))
        }
        i += 1
      }
    }
    val c = re.map(x => (0, x._1, x._2, x._3))
    (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

  def getData(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val userDayFeatures = getUserDayFeatures(spark, date)
    val userLogFeatures = getUserLogFeatures(spark, date)
    userLogFeatures.leftOuterJoin(userDayFeatures).map(x => {
      val uid = x._1
      val dense = x._2._1.toSeq
      val sparseResult = sparseVector(x._2._2.orNull)
      (uid, dense, sparseResult)
    }).zipWithUniqueId()
      .map { x =>
        (x._2,
          x._1._1,
          x._1._2,
          x._1._3._1,
          x._1._3._2,
          x._1._3._3,
          x._1._3._4
        )
      }
      .toDF("sample_idx", "uid",
        "u_dense", "u_idx0", "u_idx1", "u_idx2", "u_id_arr")
  }
}