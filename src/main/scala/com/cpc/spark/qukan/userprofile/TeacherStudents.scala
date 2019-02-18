package com.cpc.spark.qukan.userprofile


import java.sql.Timestamp

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{QttProfile, UserProfile}

/**
  * Created by roydong on 12/07/2018.
  */
@deprecated
object TeacherStudents {

  def main(args: Array[String]): Unit = {

    val date = args(0)
    val n = 20
    val spark = SparkSession
      .builder()
      .appName("teacher students " + date)
      .enableHiveSupport().getOrCreate()

    val sql =
      """
        |select device_code, member_id, nickname, wx_nickname, teacher_id, update_time
        | from gobblin.qukan_p_member_info where day = "%s"
      """.stripMargin.format(date)

    val users = spark.sql(sql).rdd
      .map {
        r =>
          val devid = r.getAs[String]("device_code")
          val mid = r.getAs[Long]("member_id")
          val nickname = r.getAs[String]("nickname")
          val wxname = r.getAs[String]("wx_nickname")
          val tmid = r.getAs[Long]("teacher_id")
          val uptime = r.getAs[Timestamp]("update_time").getTime

          (devid, mid, nickname, wxname, tmid, uptime)
      }
      .filter(x => x._1.length > 0 && x._2 > 0)
      .cache()

    println(users.count())
    users.take(10).foreach(println)

    val mid = users.map(x => (x._2, x))
    val teacher = users.filter(_._5 > 0).map(x => (x._5, x._2)).join(mid).map(x => x._2)
    val students = users.map(x => (x._5, Seq(x)))
      .filter(_._1 > 0)
      .reduceByKey(_ ++ _)
      .map {
        x =>
          (x._1, x._2.sortBy(v => -v._6).take(n))
      }
    val siblings = students.filter(_._2.length > 1).flatMap(x => x._2.map(v => (v._2, x._2)))

    val ts = users.repartition(1000)
      .map {
        x =>
          (x._2, x)
      }
      .leftOuterJoin(teacher)
      .leftOuterJoin(students)
      .map {
        x =>
          val me = x._2._1._1
          val teacher = x._2._1._2
          val students = x._2._2
          (x._1, (me, teacher, students))
      }
      .leftOuterJoin(siblings)
      .map {
        x =>
          val me = x._2._1._1
          val teacher = x._2._1._2
          val students = x._2._1._3
          val siblings = x._2._2
          (me, teacher, students, siblings)
      }

    ts.filter(x => x._2.isDefined && x._3.isDefined && x._4.isDefined).take(10).foreach(println)

    val sum = ts.repartition(200)
      .mapPartitions {
        p =>
          var n = 0
          val conf = ConfigFactory.load()
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            x =>
              val me = x._1

              val key = me._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                n = n + 1
                val user = UserProfile.parseFrom(buffer).toBuilder
                val qtt = user.getQttProfile.toBuilder
                qtt.setDevid(me._1)
                qtt.setMemberId(me._2)
                qtt.setNickname(me._3)
                qtt.setWxNickname(me._4)

                if (x._2.isDefined) {
                  val t = x._2.get
                  val teacher = qtt.getTeacher.toBuilder
                  teacher.setDevid(t._1)
                  teacher.setMemberId(t._2)
                  teacher.setNickname(t._3)
                  teacher.setWxNickname(t._4)
                  qtt.setTeacher(teacher)
                }

                if (x._3.isDefined) {
                  qtt.clearStudents()
                  x._3.get.foreach {
                    v =>
                      val s = QttProfile.newBuilder()
                      s.setDevid(v._1)
                      s.setMemberId(v._2)
                      s.setNickname(v._3)
                      s.setWxNickname(v._4)
                      qtt.addStudents(s)
                  }
                }

                if (x._4.isDefined) {
                  qtt.clearSiblings()
                  x._4.get.foreach {
                    v =>
                      //排除自己
                      if (v._2 != me._2) {
                        val s = QttProfile.newBuilder()
                        s.setDevid(v._1)
                        s.setMemberId(v._2)
                        s.setNickname(v._3)
                        s.setWxNickname(v._4)
                        qtt.addSiblings(s)
                      }
                  }
                }

                user.setQttProfile(qtt)
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq(n).iterator
      }

    println("update", sum.sum)
  }
}
