package com.cpc.spark.qukan.interest
import java.io.{FileWriter, PrintWriter}
import java.sql.{DriverManager, ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.train.LRIRModel
import com.cpc.spark.qukan.parser.HdfsParser
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.corpus.tag.Nature
import com.typesafe.config.ConfigFactory
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.json4s._
import org.json4s.native.JsonMethods._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random
import com.redis.serialization.Parse.Implicits._
import com.redis.RedisClient
import com.cpc.spark.qukan.parser.HdfsParser
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.util.control._
object CheckStudentTag {
  def main(args: Array[String]): Unit = {
    val uid = args(0).split(",")
    val detail = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag user by zfb")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
    uid.foreach {
      u =>
       var key = u + "_UPDATA"
        println(key)
       val buffer = redis.get[Array[Byte]](key).orNull
       if (buffer != null) {
         val user = UserProfile.parseFrom(buffer).toBuilder
         var t224 = false
         var t225 = false
         var t226 = false
         var t228 = false
         for (i <- 0 until user.getInterestedWordsCount) {
           val w = user.getInterestedWords(i)
           if (w.getTag == 224 && !t224) {
             println("student_tag")
             t224 = true
           }
           if (w.getTag == 225 && !t225) {
             println("not_student_tag")
             t225 = true
           }
           if (w.getTag == 226 && !t226) {
             println("active_tag")
             t226 = true
           }
           if (w.getTag == 228 && !t228) {
             t228 = true
             println("not new user")
           }
         }
         if (detail) {
           println(user)
         }
       }
    }
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -10)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, 10)
    val edate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)



    var stmt =
      """
        |select distinct member_id, create_time, device_code as did from dl_cpc.qukan_p_member_info
        |where day >= "%s" and day < "%s" and device_code = "%s"
      """.stripMargin.format(sdate, edate, uid(0))

    val zfb = spark.sql(stmt).rdd.map {
      r =>
        val member_id = r.getAs[Long](0)
        val create_time = r.getAs[Timestamp](1)
        val did = r.getAs[String](2)
        (member_id, (create_time, did))
    }.toDF("member_id", "data")
    println(stmt, zfb.count(), zfb.first())

     stmt =
      """
        |select distinct member_id, info from  gobblin.qukan_member_zfb_log
        | where update_time >= "2018-01-01" and member_id = %s
      """.stripMargin.format(zfb.first().get(0))


    val qtt = spark.sql(stmt).rdd.map{
      r =>
        val member_id = r.getAs[Long](0)
        val info = r.getAs[String](1)
        (member_id, info)
    }.toDF("member_id", "info")
    println(stmt, qtt.count(), qtt.first())

    val data = qtt.join(zfb, Seq("member_id"))
      .map {
        r =>
          val did = r.getAs[String]("did")
          val info = r.getAs[String]("info")

          val rs:Seq[(String, String, String, String, String, String)] = for {
            JObject(json) <- parse(info)
            JField("city", JString(city)) <- json
            JField("gender", JString(sex)) <- json
            JField("is_student_certified", JString(isStudent)) <- json
            JField("person_birthday", JString(birth)) <- json
            JField("province", JString(province)) <- json

            p = (did, sex, birth, isStudent, city, province)
          } yield p

          if (rs.length > 0) {
            val v = rs.head
            (v._1, v)
          } else {
            null
          }
      }
      .filter(_ != null)
      .rdd
      .reduceByKey((x, y) => x)
      .map(_._2)
    println(data.count())
    data.toLocalIterator.foreach(println)

  }
}
