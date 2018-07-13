package com.cpc.spark.qukan.interest

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.linalg.Vectors
import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.util.control._
import ml.dmlc.xgboost4j.scala.spark._

import scala.collection.mutable

/**
  * Created by myt on 13/07/2018.
  */
object ActiveUserByLX {

  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("active user by lx".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val stmt =
      """
        |select distinct uid, ext_int from dl_cpc.cpc_union_log where `date` >= "%s"
      """.stripMargin

    val rs = spark.sql(stmt).rdd.map{
      r =>
        val did = r.getAs[String](0)
        val ext = r.getMap[String, Long](1)
        val lx = ext.get("lx_package")
        (did, lx)
    }

    val conf = ConfigFactory.load()
    val sum = rs.repartition(500)
      .mapPartitions {
        p =>
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.map {
            x =>
              val key = x._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                var has = false
                var newUser = true
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == 226) {
                    has = true
                  }
                  if (w.getTag == 228) {
                    newUser = false
                  }
                }
                if (has && newUser) {
                  (x._2, (1, 1))
                } else {
                  (x._2, (0, 1))
                }
              } else {
                null
              }
          }.filter(_ != null)
      }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(x => (x._1, x._2._1, x._2._2, 1d * x._2._1 / x._2._2))
      .sortBy(_._4, false)
      sum.take(50).foreach(println)
  }
}
