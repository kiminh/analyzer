package com.cpc.spark.ml.novel

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.DnnMultiHot
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * 统计每天的用户行为存入redis
  * created time : 2018/10/24 15:45
  *
  * @author zhj
  * @version 1.0
  *
  */
object Behavior2RedisNovel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val date = args(0) //today

    val data = spark.sql(
      s"""
         |select uid,
         |       collect_set(if(load_date='${getDay(date, 1)}',show_ideaid,null)) as s_ideaid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',show_adclass,null)) as s_adclass_1,
         |       collect_set(if(load_date='${getDay(date, 2)}',show_ideaid,null)) as s_ideaid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',show_adclass,null)) as s_adclass_2,
         |       collect_set(if(load_date='${getDay(date, 3)}',show_ideaid,null)) as s_ideaid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',show_adclass,null)) as s_adclass_3,
         |
         |
         |       collect_set(if(load_date='${getDay(date, 1)}',click_ideaid,null)) as c_ideaid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_adclass,null)) as c_adclass_1,
         |
         |       collect_set(if(load_date='${getDay(date, 2)}',click_ideaid,null)) as c_ideaid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_adclass,null)) as c_adclass_2,
         |
         |       collect_set(if(load_date='${getDay(date, 3)}',click_ideaid,null)) as c_ideaid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_adclass,null)) as c_adclass_3,
         |
         |       collect_set(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 4)}',click_ideaid,null)) as c_ideaid_4_7,
         |       collect_list(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 4)}',click_adclass,null)) as c_adclass_4_7
         |
         |from dl_cpc.cpc_user_behaviors_novel
         |where load_date in ('${getDays(date, 1, 7)}')
         |    and rn <= 1000
         |group by uid
      """.stripMargin)
      .select(
        $"uid",
        hashSeq("m2", "int")($"s_ideaid_1").alias("m2"),
        hashSeq("m3", "int")($"s_ideaid_2").alias("m3"),
        hashSeq("m4", "int")($"s_ideaid_3").alias("m4"),
        hashSeq("m5", "int")($"s_adclass_1").alias("m5"),
        hashSeq("m6", "int")($"s_adclass_2").alias("m6"),
        hashSeq("m7", "int")($"s_adclass_3").alias("m7"),
        hashSeq("m8", "int")($"c_ideaid_1").alias("m8"),
        hashSeq("m9", "int")($"c_ideaid_2").alias("m9"),
        hashSeq("m10", "int")($"c_ideaid_3").alias("m10"),
        hashSeq("m11", "int")($"c_adclass_1").alias("m11"),
        hashSeq("m12", "int")($"c_adclass_2").alias("m12"),
        hashSeq("m13", "int")($"c_adclass_3").alias("m13"),
        hashSeq("m14", "int")($"c_adclass_4_7").alias("m14"),
        hashSeq("m15", "int")($"c_adclass_4_7").alias("m15")
      )
      .persist()

    println("dnn novel 用户行为特征总数：" + data.count())

    data.coalesce(20).write.mode("overwrite")
      .parquet("/user/cpc/wy/novel_behavior")


    val conf = ConfigFactory.load()
    data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      p.foreach { rec =>
        var group = Seq[Int]()
        var hashcode = Seq[Long]()
        val uid = "n1_" + rec.getString(0)
        for (i <- 1 to 14) {
          val f = rec.getAs[Seq[Long]](i)
          group = group ++ Array.tabulate(f.length)(x => i)
          hashcode = hashcode ++ f
        }
        redis.setex(uid, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
      }

      redis.disconnect
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

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @param t      ：类型
    * @return
    */
  private def hashSeq(prefix: String, t: String) = {
    t match {
      case "int" => udf {
        seq: Seq[Int] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
      case "string" => udf {
        seq: Seq[String] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
    }
  }
}
