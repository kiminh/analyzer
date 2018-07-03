package com.cpc.spark.qukan.interest

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  * Created by roydong on 11/06/2018.
  */
object PredictAge {

  val words_fnum = 41e4

  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("age model".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val sample = spark.read.parquet("/user/cpc/qtt-age-sample/p1").rdd
      .map {
        r =>
          val did = r.getAs[String]("did")
          val brand = r.getAs[Int]("brand")
          val apps = r.getAs[Seq[Row]]("apps")


          val size = 1002
          var els = Seq[(Int, Double)]()
          if (apps != null && apps.length > 0) {
            apps.foreach {
              app =>
                els = els :+ (app.getInt(1), 1d)
            }

          }
          if (brand != null) {
            els = els :+ (1000, brand * 1d)
          }
          if (apps != null) {
            els = els :+ (1001, brand * 1d)
          }
          val vec = Vectors.sparse(size.toInt, els)
          (did, vec)
      }
        .toDF("did", "features")

    println(sample.count())
    sample.rdd.take(10).foreach(println)
    var lr: LogisticRegressionModel = null
    lr = LogisticRegressionModel.load("/user/cpc/qtt-age-model/%s".format(days))
    val result = lr.transform(sample)
    val f = args(1).toDouble
    val m = args(2).toDouble
    val conf = ConfigFactory.load()
    val sum = result.rdd.map {
        r =>
          val did = r.getAs[String]("did")
          val score = r.getAs[Vector]("probability").toArray
          (did, score(1))
      }
      .filter(x => x._2 > m)
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          var n2 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

          p.foreach {
            r =>
              n += 1
              val key = r._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer != null) {
                var age = 0
                if (r._2 > m) {
                  age = 225
                } else if (r._2 < f){
                  age = 224
                }
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(age)
                  .setScore(100)
                var has = false
                for (i <- 0 to user.getInterestedWordsCount - 1) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      user.setInterestedWords(i, in)
                      n1 = n1 + 1
                      has = true
                    } else {
                      //user.removeInterestedWords(i)
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n2 = n2 + 1
                }
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((0, n), (1, n1), (2, n2)).iterator
      }

    //统计数据
    var n = 0
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n = x._2
          } else if (x._1 == 1) {
            n1 = x._2
          } else {
            n2 = x._2
          }
      }
    println("total: %s, hit: %s, update %s".format(n, n1, n2))
  }
}
