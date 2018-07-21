package com.cpc.spark.qukan.interest

import com.cpc.spark.qukan.userprofile.SetUserProfileTag
import com.cpc.spark.qukan.utils.Udfs
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import userprofile.Userprofile.{InterestItem, UserProfile}
import com.redis.serialization.Parse.Implicits._

import scala.util.control.Breaks

object PredictNoClickUser {

  def main(args: Array[String]): Unit = {


    var test_dt = args(0)

    println(s"test_dt = $test_dt")

    val featureSize: Int = 600

    val sparkApp = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("predict-noclick-user".format())
      .enableHiveSupport()
      .getOrCreate()

    var test = sparkApp.table("dl_cpc.cpc_click_user_sample_libsvm")
      .filter("adslot_type=1")
      .filter(s"dt='$test_dt'")
      .withColumn("features", Udfs.libsvmToVec(featureSize)(col("libsvm")))

    println(s"test size = ${test.count()}")
    val modelPath = "/user/cpc/qtt-noclick-user-model/1day/"

    // load
    println("load")
    val model2 = XGBoostModel.load(modelPath)
    val result = model2.transform(test)

    val thresh = 0.95
    val tag = 234
    val predict = result.rdd.map {
      r =>
        val uid = r.getAs[String]("uid")
        val score = r.getAs[DenseVector]("probabilities")
        val prob = score.values(1)
        if (prob >= thresh) {
          (uid, tag, true)
        } else {
          (uid, tag, false)
        }
        // (uid, score.values(1))
    }


    predict.take(10).foreach(println)

    //res.write.mode("overwrite").saveAsTable("test.noclicktest1")

    SetUserProfileTag.setUserProfileTag(predict)

//    val conf = ConfigFactory.load()
//    var count = 0
//    val sum = predict.repartition(500)
//      .mapPartitions {
//        p =>
//          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
//          p.foreach {
//            r =>
//              val key = r._1 + "_UPDATA"
//              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
//              if (buffer != null) {
//
//                val user = UserProfile.parseFrom(buffer).toBuilder
//
//                // delete first
//                var idx = 0
//                while (idx < user.getInterestedWordsCount) {
//                  val w = user.getInterestedWords(idx)
//                  if (w.getTag == tag) {
//                    user.removeInterestedWords(idx)
//                    idx += 100000
//                  } else {
//                    idx += 1
//                  }
//                }
//
//                // insert then
//                if (r._2 >= thresh) {
//                  val in = InterestItem.newBuilder()
//                    .setTag(tag)
//                    .setScore(100)
//                  user.addInterestedWords(in)
//                  count = count + 1
//                }
//                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
//              }
//          }
//          Seq().iterator
//      }
//    println(s"tag count: $count")
  }


}
