package com.cpc.spark.qukan.interest

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import userprofile.Userprofile.UserProfile

/**
  * Created by myt on 18/07/2018.
  */
@deprecated
object PredictSex {

  val word_num = 44e4
  val app_num = 1000

  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("predict sex".format())
      .enableHiveSupport()
      .getOrCreate()

    val sample = spark.read.parquet("/user/cpc/qtt-sex-sample/p1").rdd
      .map {
        r =>
          val did = r.getAs[String]("did")
          val score = r.getAs[Double]("score")
          val apps = r.getAs[Seq[Row]]("apps")
          val brand = r.getAs[Int]("brand")
          val w_els = r.getAs[Seq[Row]]("els")
          val size = word_num + app_num

          var els = Seq[(Int, Double)]()
          if (w_els != null) {
            els = els ++ w_els.map{x => (x.getAs[Int](0), x.getAs[Double](1))}
          }
          if (apps != null) {
            apps.foreach {
              app =>
                els = els :+ ((app.getInt(1) + word_num).toInt, 1d)
            }
          }

          if (brand != null) {
            els = els :+ (size.toInt, brand * 1d)
          }
          if (score != null) {
            els = els :+ ((size + 1).toInt, score)
          }

          val vec = Vectors.sparse((size + 2).toInt, els)
          (did, vec)
      }

    println(sample.count())
    sample.take(1).foreach(println)
    var lr: LogisticRegressionModel = null
    lr = LogisticRegressionModel.load(spark.sparkContext, "/user/cpc/qtt-sex-model/%s".format(days))
    lr.clearThreshold()
    val f = args(1).toDouble
    val m = args(2).toDouble
    val conf = ConfigFactory.load()
    val sum = sample.map(x => (x._1, lr.predict(x._2)))
      .filter(x => x._2 < f || x._2 > m)
      .repartition(100)
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          var n2 = 0
          var n3 = 0
          var n4 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

          p.foreach {
            r =>
              n += 1
              val key = r._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer != null) {
                var up : UserProfile.Builder = null
                n1 += 1
                var sex = 0
                if (r._2 < f) {
                  sex = 2
                  n3 += 1
                } else if (r._2 > m) {
                  sex = 1
                  n4 += 1
                }
                up = UserProfile.parseFrom(buffer).toBuilder()
                if (up.getSex != sex) {
                  up.setSex(sex)
                  if (up.getSex > 0) {
                    n2 += 1
                  }
                  redis.setex(key, 3600 * 24 * 7, up.build().toByteArray)
                }
              }
          }
          Seq((0, n), (1, n1), (2, n2), (3, n3)).iterator
      }

    //统计数据
    var n = 0
    var n1 = 0
    var n2 = 0
    var n3 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(4)
      .foreach {
        x =>
          if (x._1 == 0) {
            n = x._2
          } else if (x._1 == 1) {
            n1 = x._2
          } else if (x._1 == 2) {
            n2 = x._2
          } else {
            n3 = x._2
          }
      }
    println("total: %s, hit: %s, update: %s female: %s".format(n, n1, n2,n3))
  }
}

