package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.DnnMultiHot
import org.apache.spark.sql.SparkSession

/**
  * 统计每天的用户行为存入redis
  * created time : 2018/10/24 15:45
  *
  * @author zhj
  * @version 1.0
  *
  */
object Behavior2Redis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)

    val data = spark.sql(
      s"""
         |select uid,
         |       collect_list(if(load_date='${getDay(date, 1)}',show_ideaid,null)) as s_ideaid_1,
         |       collect_list(if(load_date='${getDay(date, 1)}',show_adclass,null)) as s_adclass_1,
         |       collect_list(if(load_date='${getDay(date, 2)}',show_ideaid,null)) as s_ideaid_2,
         |       collect_list(if(load_date='${getDay(date, 2)}',show_adclass,null)) as s_adclass_2,
         |       collect_list(if(load_date='${getDay(date, 3)}',show_ideaid,null)) as s_ideaid_3,
         |       collect_list(if(load_date='${getDay(date, 3)}',show_adclass,null)) as s_adclass_3,
         |       collect_list(if(load_date='${getDay(date, 1)}',click_ideaid,null)) as c_ideaid_1,
         |       collect_list(if(load_date='${getDay(date, 1)}',click_adclass,null)) as c_adclass_1,
         |       collect_list(if(load_date='${getDay(date, 2)}',click_ideaid,null)) as c_ideaid_2,
         |       collect_list(if(load_date='${getDay(date, 2)}',click_adclass,null)) as c_adclass_2,
         |       collect_list(if(load_date='${getDay(date, 3)}',click_ideaid,null)) as c_ideaid_3,
         |       collect_list(if(load_date='${getDay(date, 3)}',click_adclass,null)) as c_adclass_3
         |from dl_cpc.cpc_user_behaviors
         |where load_date in ('${getDays(date, 1, 3)}')
         |    and rn <= 1000
         |group by uid
         |limit 100
      """.stripMargin)

    val conf = ConfigFactory.load()
    /*data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      p.foreach { rec =>
        var group = Seq[Int]()
        var hashcode = Seq[Long]()
        val uid = "dnnu" + rec.getString(0)
        for (i <- 1 to 12) {
          val f = rec.getAs[Seq[Int]](i).map(_.toLong)
          group = group ++ Array.tabulate(f.length)(x => i)
          hashcode = hashcode ++ f
        }
        //redis.set(uid, DnnMultiHot(group, hashcode).toByteArray)
        println(uid, DnnMultiHot(group, hashcode).toString)
      }

      redis.disconnect
    }*/
    data.collect.foreach { rec =>
      var group = Seq[Int]()
      var hashcode = Seq[Long]()
      val uid = "dnnu" + rec.getString(0)
      for (i <- 1 to 12) {
        val f = rec.getAs[Seq[Int]](i).map(_.toLong)
        group = group ++ Array.tabulate(f.length)(x => i)
        hashcode = hashcode ++ f
      }
      //redis.set(uid, DnnMultiHot(group, hashcode).toByteArray)
      println(uid, DnnMultiHot(group, hashcode).toString)
    }
  }

  /**
    * 获取时间序列
    *
    * @param startdate : 日期
    * @param day1      ：日期之前day1天作为开始日期
    * @param day2      ：日期序列数量
    * @return
    */
  def getDays(startdate: String, day1: Int = 0, day2: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day1)
    var re = Seq(format.format(cal.getTime))
    for (i <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re.mkString("','")
  }

  /**
    * 获取时间
    *
    * @param startdate ：开始日期
    * @param day       ：开始日期之前day天
    * @return
    */
  def getDay(startdate: String, day: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day)
    format.format(cal.getTime)
  }
}
