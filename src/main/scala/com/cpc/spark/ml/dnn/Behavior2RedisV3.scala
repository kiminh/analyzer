package com.cpc.spark.ml.dnn

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
object Behavior2RedisV3 {
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
         |       collect_set(if(load_date='${getDay(date, 1)}',click_userid,null)) as c_userid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_planid,null)) as c_planid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_adtype,null)) as c_adtype_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_interaction,null)) as c_interaction_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_city,null)) as c_city_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_adslotid,null)) as c_adslotid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_phone_level,null)) as c_phone_level_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',click_brand_title,null)) as c_brand_title_1,
         |
         |       collect_set(if(load_date='${getDay(date, 2)}',click_ideaid,null)) as c_ideaid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_adclass,null)) as c_adclass_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_userid,null)) as c_userid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_planid,null)) as c_planid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_adtype,null)) as c_adtype_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_interaction,null)) as c_interaction_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_city,null)) as c_city_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_adslotid,null)) as c_adslotid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_phone_level,null)) as c_phone_level_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',click_brand_title,null)) as c_brand_brand_title_2,
         |
         |       collect_set(if(load_date='${getDay(date, 3)}',click_ideaid,null)) as c_ideaid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_adclass,null)) as c_adclass_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_userid,null)) as c_userid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_planid,null)) as c_planid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_adtype,null)) as c_adtype_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_interaction,null)) as c_interaction_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_city,null)) as c_city_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_adslotid,null)) as c_adslotid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_phone_level,null)) as c_phone_level_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',click_brand_title,null)) as c_brand_title_3
         |
         |from dl_cpc.cpc_user_behaviors
         |where load_date in ('${getDays(date, 1, 3)}')
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

        //d3新加特征
        hashSeq("m14", "int")($"c_userid_1").alias("m14"),
        hashSeq("m15", "int")($"c_userid_2").alias("m15"),
        hashSeq("m16", "int")($"c_userid_3").alias("m16"),
        hashSeq("m17", "int")($"c_planid_1").alias("m17"),
        hashSeq("m18", "int")($"c_planid_2").alias("m18"),
        hashSeq("m19", "int")($"c_planid_3").alias("m19"),
        hashSeq("m20", "int")($"c_adtype_1").alias("m20"),
        hashSeq("m21", "int")($"c_adtype_2").alias("m21"),
        hashSeq("m22", "int")($"c_adtype_3").alias("m22"),
        hashSeq("m23", "int")($"c_interaction_1").alias("m23"),
        hashSeq("m24", "int")($"c_interaction_2").alias("m24"),
        hashSeq("m25", "int")($"c_interaction_3").alias("m25"),
        hashSeq("m26", "int")($"c_city_1").alias("m26"),
        hashSeq("m27", "int")($"c_city_2").alias("m27"),
        hashSeq("m28", "int")($"c_city_3").alias("m28"),
        hashSeq("m29", "string")($"c_adslotid_1").alias("m29"),
        hashSeq("m30", "string")($"c_adslotid_2").alias("m30"),
        hashSeq("m31", "string")($"c_adslotid_3").alias("m31"),
        hashSeq("m32", "int")($"c_phone_level_1").alias("m32"),
        hashSeq("m33", "int")($"c_phone_level_2").alias("m33"),
        hashSeq("m34", "int")($"c_phone_level_3").alias("m34"),
        hashSeq("m35", "string")($"c_brand_title_1").alias("m35"),
        hashSeq("m36", "string")($"c_brand_title_2").alias("m36"),
        hashSeq("m37", "string")($"c_brand_title_3").alias("m37")
      )
      .persist()

    println("dnn v2 用户行为特征总数：" + data.count())

    data.coalesce(20).write.mode("overwrite")
      .parquet("/user/cpc/zhj/behaviorV3")


    /*val conf = ConfigFactory.load()
    data.coalesce(20).foreachPartition { p =>
      val redis = new RedisClient(conf.getString("ali_redis.host"), conf.getInt("ali_redis.port"))
      redis.auth(conf.getString("ali_redis.auth"))

      p.foreach { rec =>
        var group = Seq[Int]()
        var hashcode = Seq[Long]()
        val uid = "d3_" + rec.getString(0)
        for (i <- 1 to 12) {
          val f = rec.getAs[Seq[Long]](i)
          group = group ++ Array.tabulate(f.length)(x => i)
          hashcode = hashcode ++ f
        }
        redis.setex(uid, 3600 * 24 * 7, DnnMultiHot(group, hashcode).toByteArray)
      }

      redis.disconnect
    }*/
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
