package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfileV2}
import redis.clients.jedis.{HostAndPort, JedisCluster}
import scala.util.control._

object Lab2 {
  def main(args: Array[String]): Unit = {
    val op = args(0).toInt
    val date = args(1).toString
    val hour = {if (op == 0) {""} else {args(2).toString}}


    val spark = SparkSession.builder()
      .appName("TagFromHivetoNewRedis")
      .enableHiveSupport()
      .getOrCreate()

    var stmt = ""
    if (op == 0) {
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_daily where `date` = "%s"
        """.stripMargin.format(date)
    } else if (op == 1){
      stmt =
        """
          |select uid, tag, operation from dl_cpc.cpc_userprofile_tag_hourly where `date` = "%s" and `hour` = "%s"
        """.stripMargin.format(date, hour)
    }
    if (stmt != "") {
      println(stmt)
      val rs = spark.sql(stmt).rdd.repartition(5000).map{
        r =>
          ((r.getAs[String](0), r.getAs[Boolean](2)), Seq(r.getAs[Int](1)))
      }.reduceByKey(_++_).repartition(5000).map(x => (x._1._1, Seq((x._1._2, x._2)))).reduceByKey(_++_)
      rs.cache().take(3).foreach(println)
      rs.repartition(200).foreachPartition(
        p =>{
          var del = 0
          var ins = 0
          var hit = 0
          var tot = 0
          val redisV2 = new JedisCluster(new HostAndPort("192.168.80.152", 7003))
          val loop = new Breaks
          var ret = Seq[(String, Int)]()
          val cnt = p.foreach{
            x =>
              tot += 1
              val key = x._1 + "_upv2"
              val toDel = x._2.filter(p => p._1 == false).flatMap(x => x._2).distinct
              val toAdd = x._2.filter(p => p._1 == true).flatMap(x => x._2).distinct
              val buffer = redisV2.get(key.getBytes)
              if (buffer != null) {
                hit += 1
                var user: UserProfileV2.Builder = null
                //user.mergeFrom(buffer)
                try {
                  user = UserProfileV2.parseFrom(buffer).toBuilder
                } catch {
                  case ex: Exception => redisV2.del(key.getBytes)
                }
                if(user != null) {
                  loop.breakable {
                    var idx = 0
                    while (idx < user.getInterestedWordsCount) {
                      val w = user.getInterestedWords(idx)
                      if (toDel.contains(w.getTag)) {
                        user.removeInterestedWords(idx)
                        del += 1
                      } else if (toAdd.contains(w.getTag)) {
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
                    user.clearInterests()
                  }
                  redisV2.setex(key.getBytes, 3600 * 24 * 14, user.build().toByteArray)
                }
              } else if (buffer == null) {
                val user = UserProfileV2.newBuilder().setUid(x._1)
                for (i <- toAdd) {
                  ins += 1
                  val interest = InterestItem.newBuilder()
                    .setTag(i)
                    .setScore(100)
                  user.addInterestedWords(interest)
                }
                redisV2.setex(key.getBytes, 3600 * 24 * 14, user.build().toByteArray)
              }
          }
        }
      )
    }}


}
