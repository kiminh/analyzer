package com.cpc.spark.qukan.interest

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
import userprofile.Userprofile.{InterestItem, UserProfile}
import scala.util.control._
import com.cpc.spark.qukan.userprofile.SetUserProfileTag

object TagUserByPV {
  def main(args: Array[String]): Unit = {
    val days  = args(0).toInt
    val is_set = args(1).toBoolean
    val spark = SparkSession.builder()
      .appName("Tag user by PV")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -days)
    val sdate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, days - 1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val stmt =
      """
        |select distinct device,read_pv_inner,day from bdm.qukan_daily_active_extend_p_all_bydevice where day >= "%s"
      """.stripMargin.format(sdate)

    val read = spark.sql(stmt).rdd.map{
      r =>
        val uid = r.getAs[String](0)
        val read = r.getAs[Int](2)
        (uid, read)
    }.reduceByKey(_+_)
    println(read.count())
    val stmt2 =
      """
        |select distinct uid,count(distinct searchid) from dl_cpc.cpc_union_log
        |where `date` >= "%s" and `date` <= "%s" and media_appsid in ("80000001","80000002") and isclick = 1
      """.stripMargin.format(sdate, yesterday)

    val ad = spark.sql(stmt2).rdd.map {
      r =>
        val uid = r.getAs[String](0)
        val ad = r.getAs[Int](1)
        (uid, ad)
    }
    println(ad.count())
    val rs = ad.join(read).map {
      r =>
        (r._1, 1d * r._2._2 / r._2._1)
    }
    println(rs.count())
    println(rs.filter(_._2 > 0.5).count())
    val conf = ConfigFactory.load()
    val toSet = rs.repartition(100)
      .mapPartitions{
        p =>
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.flatMap {
            r =>
              var is235 = false
              var is236 = false
              val key = r._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                for (i <- 0 until user.getInterestedWordsCount ) {
                  val w = user.getInterestedWords(idx)
                  if (w.getTag == 235) {
                    is235 = true
                  }
                  if (w.getTag == 236) {
                    is236 = true
                  }
                }
              }
              if (!is236 && r._2 > 0.5) {
                if (is235) {
                  Seq((r._1, 238, true),(r._1, 237, true))
                } else {
                  Seq((r._1, 237, true))
                }
              } else {
                Seq((r._1, 237, false), (r._1, 238, false))
              }
          }
      }
    SetUserProfileTag.setUserProfileTag(toSet)
  }
}
