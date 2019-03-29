package com.cpc.spark.ocpcV3.HP

import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, Row}
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import userprofile.Userprofile.InterestItem
import scala.util.control._
import com.cpc.spark.qukan.userprofile.SetUserProfileTag

object Lab3 {
  def main(args: Array[String]): Unit = {
    val op = args(0).toInt
    val date = args(1).toString
    val hour = {if (op == 0) {""} else {args(2).toString}}
    val tag = {if (op == 0 || op == 1) {""} else {args(3).toInt}}

    println(op, date, hour, tag)

    val spark = SparkSession.builder()
      .appName("TagFromHivetoRedis")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    var stmt = ""
    if (op == 0) {0
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_daily where `date` = "%s"
        """.stripMargin.format(date)
    } else if (op == 1){
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and `hour` = "%s"
        """.stripMargin.format(date, hour)
    } else if (op == 2) {
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_daily where `date` = "%s" and tag = %s
        """.stripMargin.format(date, tag)
    } else if (op == 3) {
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and `hour` = "%s" and tag = %s
        """.stripMargin.format(date, hour, tag)
    }
    if (stmt != "") {
      println(stmt)
      val rs = spark.sql(stmt).rdd.map{
        r =>
          ((r.getAs[String](0), r.getAs[Boolean](2)), Seq(r.getAs[Int](1)))
      }.reduceByKey(_++_)
        .map(x => (x._1._1, Seq((x._1._2, x._2))))
        .reduceByKey(_++_)
      rs.take(3).foreach(println)
      val conf = ConfigFactory.load()
      val sum = rs.repartition(200)
        .mapPartitions{
          p =>
            var del = 0
            var ins = 0
            var hit = 0
            var tot = 0
            val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
            val loop = new Breaks
            var ret = Seq[(String, Int)]()
            val cnt = p.foreach{
              x =>
                tot += 1
                val key = x._1 + "_UPDATA"
                val toDel = x._2.filter(p => p._1 == false).flatMap(x => x._2).distinct
                val toAdd = x._2.filter(p => p._1 == true).flatMap(x => x._2).distinct
                val buffer = redis.get[Array[Byte]](key).orNull
                if (buffer != null) {
                  hit += 1
                  val user = UserProfile.parseFrom(buffer).toBuilder
                  loop.breakable {
                    var idx = 0
                    while (idx < user.getInterestedWordsCount) {
                      val w = user.getInterestedWords(idx)
                      if (toDel.contains(w.getTag)) {
                        user.removeInterestedWords(idx)
                        del += 1
                      } else if (toAdd.contains(w.getTag)){
                        user.removeInterestedWords(idx)
                      } else {
                        idx += 1
                      }
                      if (idx == user.getInterestedWordsCount) {
                        loop.break()
                      }
                    }
                  }
                  for (i <- toAdd) {
                    ins += 1
                    val interest = InterestItem.newBuilder()
                      .setTag(i)
                      .setScore(100)
                    user.addInterestedWords(interest)
                  }
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                } else if (buffer == null) {
                  val user = UserProfile.newBuilder().setDevid(x._1)
                  for (i <- toAdd) {
                    ins += 1
                    val interest = InterestItem.newBuilder()
                      .setTag(i)
                      .setScore(100)
                    user.addInterestedWords(interest)
                  }
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                }
                ret = ret :+ (x._1, toDel.length + toAdd.length)
            }
            (Seq(("total", tot), ("hit", hit), ("insert", ins), ("delete", del))).iterator
        }.reduceByKey(_+_).toLocalIterator
      sum.foreach(println)
    }
  }
}
