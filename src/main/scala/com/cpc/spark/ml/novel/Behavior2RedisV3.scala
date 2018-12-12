package com.cpc.spark.ml.novel

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
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

    val date = args(0)

    // user dayily featrues
    saveUserDailyFeatures(spark, date)

    //ad daily features
//    saveAdDailyFeatures(spark, date)

  }

  private def saveUserDailyFeatures(spark: SparkSession, date: String): Unit = {
    import spark.implicits._
    //用户安装app
    val ud_sql0 =
      s"""
         |select * from dl_cpc.cpc_user_installed_apps where load_date = '${getDay(date, 1)}'
      """.stripMargin

    //用户天级别过去访问广告情况
    val ud_sql1 =
      s"""
         |select uid,
         |       collect_set(if(load_date='${getDay(date, 1)}',show_ideaid,null)) as s_ideaid_1,
         |       collect_set(if(load_date='${getDay(date, 1)}',show_adclass,null)) as s_adclass_1,
         |       collect_set(if(load_date='${getDay(date, 2)}',show_ideaid,null)) as s_ideaid_2,
         |       collect_set(if(load_date='${getDay(date, 2)}',show_adclass,null)) as s_adclass_2,
         |       collect_set(if(load_date='${getDay(date, 3)}',show_ideaid,null)) as s_ideaid_3,
         |       collect_set(if(load_date='${getDay(date, 3)}',show_adclass,null)) as s_adclass_3,
         |
         |       collect_list(if(load_date='${getDay(date, 1)}',click_ideaid,null)) as c_ideaid_1,
         |       collect_list(if(load_date='${getDay(date, 1)}',click_adclass,null)) as c_adclass_1,
         |
         |       collect_list(if(load_date='${getDay(date, 2)}',click_ideaid,null)) as c_ideaid_2,
         |       collect_list(if(load_date='${getDay(date, 2)}',click_adclass,null)) as c_adclass_2,
         |
         |       collect_list(if(load_date='${getDay(date, 3)}',click_ideaid,null)) as c_ideaid_3,
         |       collect_list(if(load_date='${getDay(date, 3)}',click_adclass,null)) as c_adclass_3,
         |
         |       collect_list(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 4)}',click_ideaid,null)) as c_ideaid_4_7,
         |       collect_list(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 4)}',click_adclass,null)) as c_adclass_4_7
         |from dl_cpc.cpc_user_behaviors_novel
         |where load_date in ('${getDays(date, 1, 7)}')
         |group by uid
      """.stripMargin

    //用户点击过的广告分词
    val ud_sql2 =
      s"""
         |select uid,
         |       interest_ad_words_1 as word1,
         |       interest_ad_words_3 as word3
         |from dl_cpc.cpc_user_interest_words_novel
         |where load_date='$date'
    """.stripMargin

    //用户点击过的文章id及分类
    val ud_sql3 =
      s"""
         |select uid,book_id,first_category_id,second_category_id,third_category_id
         |from dl_cpc.miReadTrait where day = '$date'
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
      """.stripMargin


    println("============= user daily features =============")
    println(ud_sql0)
    println("-------------------------------------------------")
    println(ud_sql1)
    println("-------------------------------------------------")
    println(ud_sql2)
    println("-------------------------------------------------")
    println(ud_sql3)

    val ud_features = spark.sql(ud_sql0).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs")
      .join(spark.sql(ud_sql1), Seq("uid"), "outer")
      .join(spark.sql(ud_sql2), Seq("uid"), "outer")
      .join(spark.sql(ud_sql3), Seq("uid"), "outer")
      .select($"uid",
        hashSeq("f30#", "string")($"pkgs").alias("f30"),
        hashSeq("ud1#", "int")($"s_ideaid_1").alias("ud1"),
        hashSeq("ud2#", "int")($"s_ideaid_2").alias("ud2"),
        hashSeq("ud3#", "int")($"s_ideaid_3").alias("ud3"),
        hashSeq("ud4#", "int")($"s_adclass_1").alias("ud4"),
        hashSeq("ud5#", "int")($"s_adclass_2").alias("ud5"),
        hashSeq("ud6#", "int")($"s_adclass_3").alias("ud6"),
        hashSeq("ud7#", "int")($"c_ideaid_1").alias("ud7"),
        hashSeq("ud8#", "int")($"c_ideaid_2").alias("ud8"),
        hashSeq("ud9#", "int")($"c_ideaid_3").alias("ud9"),
        hashSeq("ud10#", "int")($"c_adclass_1").alias("ud10"),
        hashSeq("ud11#", "int")($"c_adclass_2").alias("ud11"),
        hashSeq("ud12#", "int")($"c_adclass_3").alias("ud12"),
        hashSeq("ud13#", "int")($"c_ideaid_4_7").alias("ud13"),
        hashSeq("ud14#", "int")($"c_adclass_4_7").alias("ud14"),
        hashSeq("ud15#", "int")($"book_id").alias("ud15"),
        hashSeq("ud16#", "int")($"first_category_id").alias("ud16"),
        hashSeq("ud17#", "int")($"second_category_id").alias("ud17"),
        hashSeq("ud18#", "int")($"third_category_id").alias("ud18"),
        hashSeq("ud19#", "string")($"word1").alias("ud19"),
        hashSeq("ud20#", "string")($"word3").alias("ud20")
      ).persist()

    ud_features.coalesce(50).write.mode("overwrite")
      .parquet(s"/user/cpc/wy/novel/features/ud")

    ud_features.show()

    Utils.DnnFeatures2Redis.multiHot2Redis(ud_features, "n4_", "string")
  }

  private def saveAdDailyFeatures(spark: SparkSession, date: String): Unit = {
    import spark.implicits._
    val title_sql =
      """
        |select id as ideaid,
        |       split(tokens,' ') as words
        |from dl_cpc.ideaid_title
      """.stripMargin

    println("============= ad daily feature ============")
    println(title_sql)

    val ad_features = spark.sql(title_sql)
      .select($"ideaid",
        hashSeq("ad0#", "string")($"words").alias("ad0"))
      .persist()

    ad_features.coalesce(1).write.mode("overwrite")
      .parquet("/user/cpc/wy/novel/features/ad")

    ad_features.show()

    Utils.DnnFeatures2Redis.multiHot2Redis(ad_features, "n_id_", "int")

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
