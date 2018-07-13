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

import scala.util.control._
import ml.dmlc.xgboost4j.scala.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  * Created by myt on 13/07/2018.
  */
object ActiveUserByLX {

  def main(args: Array[String]): Unit = {
    val days = args(0).toInt
    val isread = args(1).toBoolean
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("active user by lx".format())
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    if (isread) {
      val cal = Calendar.getInstance()
      cal.add(Calendar.DATE, -days)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val stmt =
        """
          |select uid, ext_int from dl_cpc.cpc_union_log where `date` >= "%s"
        """.stripMargin.format(date)

      val rs = spark.sql(stmt).rdd.map {
        r =>
          val did = r.getAs[String](0)
          val ext = r.getMap[String, Long](1)
          val lx = ext.get("lx_package")
          if (lx != None) {
            (did, lx)
          } else {
            null
          }
      }.filter(_ != null)
        .distinct()
        .toDF("did", "lx")

      val stmt2 =
        """
          |select searchid,uid from dl_cpc.ml_cvr_feature_v1 where `date` >= "%s"
        """.stripMargin.format(date)

      val rs2 = spark.sql(stmt2).rdd.map {
        r =>
          val did = r.getAs[String](1)
          (did, 1)
      }.reduceByKey(_+_)
        .toDF("did", "cnt")

      val sum = rs.join(rs2, "leftouter").rdd.map {
        r =>
          val did = r.getAs[String]("did")
          val lx = r.getAs[Long]("lx")
          val cnt = r.getAs[Int]("sum")
          if (cnt == null) {
            (lx, (0, 1))
          } else {
            (lx, (cnt, 1))
          }
      }.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .sortBy(_._2._1)

      sum.toDF("lx", "hit", "total").write.parquet("/user/cpc/active-user-by-lx/%s".format(days))
      sum.take(50).foreach(println)
    } else {
      val sum = spark.read.parquet("/user/cpc/active-user-by-lx/%s".format(days)).rdd.map{
        r =>
          val lx = r.getAs[Long](0)
          val hit = r.getAs[Int](1)
          val total = r.getAs[Int](2)
          (lx, hit, total)
      }
      sum.take(50).foreach(println)
    }


  }
}
