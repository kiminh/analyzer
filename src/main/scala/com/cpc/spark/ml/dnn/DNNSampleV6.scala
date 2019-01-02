package com.cpc.spark.ml.dnn

import java.io.File

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import sys.process._

/**
  * d6数据生成规范化
  * created time : 2018/11/28 16:36
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNSampleV6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(trdate, trpath, tedate, tepath) = args

    val sample = new DNNSampleV6(spark, trdate, trpath, tedate, tepath)
    sample.saveTrain()
  }
}


class DNNSampleV6(spark: SparkSession, trdate: String = "", trpath: String = "",
                  tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  /**
    * as features：id 类特征的 one hot feature    ====== 前缀 f+index+"#" 如 f0#,f1#,f2#..
    *
    * @param date
    * @param adtype
    * @return
    */
  private def getAsFeature(date: String, adtype: Int = 1): DataFrame = {
    import spark.implicits._
    val as_sql =
      s"""
         |select a.searchid,
         |  if(coalesce(c.label, b.label, 0) > 0, array(1, 0), array(0, 1)) as cvr_label,
         |  if(a.isclick>0, array(1,0), array(0,1)) as label,
         |  a.media_type, a.media_appsid as mediaid,
         |  a.ext['channel'].int_value as channel,
         |  a.ext['client_type'].string_value as sdk_type,
         |
         |  a.adslot_type, a.adslotid,
         |
         |  a.adtype, a.interaction, a.bid, a.ideaid, a.unitid, a.planid, a.userid,
         |  a.ext_int['is_new_ad'] as is_new_ad, a.ext['adclass'].int_value as adclass,
         |  a.ext_int['siteid'] as site_id,
         |
         |  a.os, a.network, a.ext['phone_price'].int_value as phone_price,
         |  a.ext['brand_title'].string_value as brand,
         |
         |  a.province, a.city, a.ext['city_level'].int_value as city_level,
         |
         |  a.uid, a.age, a.sex, a.ext_string['dtu_id'] as dtu_id,
         |
         |  a.hour, a.ext_int['content_id'] as content_id,
         |  a.ext_int['category'] as content_category
         |
         |from dl_cpc.cpc_union_log a
         |left join dl_cpc.ml_cvr_feature_v1 b
         |  on a.searchid=b.searchid
         |  and b.label2=1
         |  and b.date='$date'
         |left join dl_cpc.ml_cvr_feature_v2 c
         |  on a.searchid=c.searchid
         |  and c.label=1
         |  and c.date='$date'
         |where a.`date` = '$date'
         |  and a.isshow = 1 and a.ideaid > 0 and a.adslot_type = $adtype
         |  and a.media_appsid in ("80000001", "80000002")
         |  and a.uid not like "%.%"
         |  and a.uid not like "%000000%"
         |  and length(a.uid) in (14, 15, 36)
      """.stripMargin
    println("============= as features ==============")
    println(as_sql)

    val data = spark.sql(as_sql).persist()

    data.write.mode("overwrite").parquet(s"/user/cpc/dnn/raw_data_list/$date")

    data
      .select($"label",
        $"uid",
        $"ideaid",
        hash("f0#")($"media_type").alias("f0"),
        hash("f1#")($"mediaid").alias("f1"),
        hash("f2#")($"channel").alias("f2"),
        hash("f3#")($"sdk_type").alias("f3"),
        hash("f4#")($"adslot_type").alias("f4"),
        hash("f5#")($"adslotid").alias("f5"),
        hash("f6#")($"sex").alias("f6"),
        hash("f7#")($"dtu_id").alias("f7"),
        hash("f8#")($"adtype").alias("f8"),
        hash("f9#")($"interaction").alias("f9"),
        hash("f10#")($"bid").alias("f10"),
        hash("f11#")($"ideaid").alias("f11"),
        hash("f12#")($"unitid").alias("f12"),
        hash("f13#")($"planid").alias("f13"),
        hash("f14#")($"userid").alias("f14"),
        hash("f15#")($"is_new_ad").alias("f15"),
        hash("f16#")($"adclass").alias("f16"),
        hash("f17#")($"site_id").alias("f17"),
        hash("f18#")($"os").alias("f18"),
        hash("f19#")($"network").alias("f19"),
        hash("f20#")($"phone_price").alias("f20"),
        hash("f21#")($"brand").alias("f21"),
        hash("f22#")($"province").alias("f22"),
        hash("f23#")($"city").alias("f23"),
        hash("f24#")($"city_level").alias("f24"),
        hash("f25#")($"uid").alias("f25"),
        hash("f26#")($"age").alias("f26"),
        hash("f27#")($"hour").alias("f27")
      )
      .select(
        array($"f0", $"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27")
          .alias("dense"),
        $"label",
        $"uid",
        $"ideaid"
      ).repartition(1000, $"uid")
  }

  private def getAsFeature_hourly(date: String, hour: Int, adtype: Int = 1): DataFrame = {
    import spark.implicits._
    val as_sql =
      s"""
         |select if(isclick>0, array(1,0), array(0,1)) as label,
         |  media_type, media_appsid as mediaid,
         |  ext['channel'].int_value as channel,
         |  ext['client_type'].string_value as sdk_type,
         |
         |  adslot_type, adslotid,
         |
         |  adtype, interaction, bid, ideaid, unitid, planid, userid,
         |  ext_int['is_new_ad'] as is_new_ad, ext['adclass'].int_value as adclass,
         |  ext_int['siteid'] as site_id,
         |
         |  os, network, ext['phone_price'].int_value as phone_price,
         |  ext['brand_title'].string_value as brand,
         |
         |  province, city, ext['city_level'].int_value as city_level,
         |
         |  uid, age, sex, ext_string['dtu_id'] as dtu_id,
         |
         |  hour, ext_int['content_id'] as content_id, ext_int['category'] as content_category
         |
         |from dl_cpc.cpc_union_log where `date` = '$date' and hour=$hour
         |  and isshow = 1 and ideaid > 0 and adslot_type = $adtype
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
      """.stripMargin
    println("============= as features ==============")
    println(as_sql)
    spark.sql(as_sql)
      .select($"label",
        $"uid",
        $"ideaid",

        hash("f0#")($"media_type").alias("f0"),
        hash("f1#")($"mediaid").alias("f1"),
        hash("f2#")($"channel").alias("f2"),
        hash("f3#")($"sdk_type").alias("f3"),
        hash("f4#")($"adslot_type").alias("f4"),
        hash("f5#")($"adslotid").alias("f5"),
        hash("f6#")($"sex").alias("f6"),
        hash("f7#")($"dtu_id").alias("f7"),
        hash("f8#")($"adtype").alias("f8"),
        hash("f9#")($"interaction").alias("f9"),
        hash("f10#")($"bid").alias("f10"),
        hash("f11#")($"ideaid").alias("f11"),
        hash("f12#")($"unitid").alias("f12"),
        hash("f13#")($"planid").alias("f13"),
        hash("f14#")($"userid").alias("f14"),
        hash("f15#")($"is_new_ad").alias("f15"),
        hash("f16#")($"adclass").alias("f16"),
        hash("f17#")($"site_id").alias("f17"),
        hash("f18#")($"os").alias("f18"),
        hash("f19#")($"network").alias("f19"),
        hash("f20#")($"phone_price").alias("f20"),
        hash("f21#")($"brand").alias("f21"),
        hash("f22#")($"province").alias("f22"),
        hash("f23#")($"city").alias("f23"),
        hash("f24#")($"city_level").alias("f24"),
        hash("f25#")($"uid").alias("f25"),
        hash("f26#")($"age").alias("f26"),
        hash("f27#")($"hour").alias("f27")
      )
      .select(
        array($"f0", $"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27")
          .alias("dense"),
        $"label",
        $"uid",
        $"ideaid"
      )
  }

  /**
    * user dayily featrues：用户天级别 multihot特征 ====== 前缀 ud+index+"#" 如 ud0#
    *
    * @param date
    * @return
    */
  private def getUdFeature(date: String): DataFrame = {
    import spark.implicits._
    //用户安装app
    val ud_sql0 =
      s"""
         |select * from dl_cpc.cpc_user_installed_apps where load_date = '$date'
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
         |from dl_cpc.cpc_user_behaviors
         |where load_date in ('${getDays(date, 1, 7)}')
         |group by uid
      """.stripMargin

    //用户点击过的广告分词
    val ud_sql2 =
      s"""
         |select uid,
         |       interest_ad_words_1 as word1,
         |       interest_ad_words_3 as word3
         |from dl_cpc.cpc_user_interest_words
         |where load_date='$date'
    """.stripMargin

    println("============= user dayily features =============")
    println(ud_sql0)
    println("-------------------------------------------------")
    println(ud_sql1)
    println("-------------------------------------------------")
    println(ud_sql2)


    spark.sql(ud_sql0).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs")
      .join(spark.sql(ud_sql1), Seq("uid"), "outer")
      .join(spark.sql(ud_sql2), Seq("uid"), "outer")
      .select($"uid",
        hashSeq("ud0#", "string")($"pkgs").alias("ud0"),
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
        hashSeq("ud15#", "string")($"word1").alias("ud15"),
        hashSeq("ud16#", "string")($"word3").alias("ud16")
      )
  }

  private def getUdFeature_hourly(date: String): DataFrame = {
    spark.read.parquet("/user/cpc/dnn/features/ud")
  }

  /**
    * ad daily features: 广告天级别 multihot 特征  ====== 前缀 ad+index+"#" 如 ad0#
    *
    * @param date
    * @return
    */
  private def getAdFeature(date: String = ""): DataFrame = {
    import spark.implicits._
    val title_sql =
      """
        |select id as ideaid,
        |       split(tokens,' ') as words
        |from dl_cpc.ideaid_title
      """.stripMargin

    println("============= ad daily feature ============")
    println(title_sql)

    spark.sql(title_sql)
      .select($"ideaid",
        hashSeq("ad0#", "string")($"words").alias("ad0")
      )
  }

  private def getAdFeature_hourly(date: String = ""): DataFrame = {
    spark.read.parquet("/user/cpc/dnn/features/ad")
  }

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._

    var data: DataFrame = null

    if (date.length == 10) {
      data = getAsFeature(date)
        .join(getUdFeature(date), Seq("uid"), "left")
        .join(broadcast(getAdFeature(date)), Seq("ideaid"), "left")
    }
    else if (date.length == 13) {
      val dt = date.substring(0, 10)
      val h = date.substring(11, 13).toInt
      data = getAsFeature_hourly(dt, h)
        .join(getUdFeature_hourly(date), Seq("uid"), "left")
        .join(broadcast(getAdFeature_hourly(date)), Seq("ideaid"), "left")
        .persist()
    } else {
      println(date)
      println("-------------------日期格式传入错误-----------------")
      println("       正确格式：yyyy-MM-dd 或 yyyy-MM-dd-HH        ")
      println("---------------------------------------------------")
      sys.exit(1)
    }


    //获取默认hash列表
    val columns = Seq("ud0", "ud1", "ud2", "ud3", "ud4", "ud5", "ud6", "ud7", "ud8", "ud9", "ud10",
      "ud11", "ud12", "ud13", "ud14", "ud15", "ud16", "ad0")
    val default_hash = for (col <- columns.zipWithIndex)
      yield (col._2, 0, Murmur3Hash.stringHash64(col._1 + "#", 0))

    data
      .select(
        $"label",
        $"uid",
        $"dense",
        mkSparseFeature(default_hash)(
          array($"ud0", $"ud1", $"ud2", $"ud3", $"ud4", $"ud5", $"ud6", $"ud7", $"ud8"
            , $"ud9", $"ud10", $"ud11", $"ud12", $"ud13", $"ud14", $"ud15", $"ud16", $"ad0")
        ).alias("sparse")
      )
      .select(
        hash("uid")($"uid").alias("sample_idx"),
        $"label",
        $"dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }
}
