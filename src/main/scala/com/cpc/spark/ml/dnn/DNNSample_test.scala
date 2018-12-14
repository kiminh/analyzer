package com.cpc.spark.ml.dnn

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * 在d3版本特征的基础上实时增加当天看过和点击过的广告ideaid
  * created time : 2018/11/10 10:35
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNSample_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("getFtrlFeature", getFtrlFeature _)

    spark.udf.register("hash", Murmur3Hash.stringHash64 _)
    spark.udf.register("hashSeq", hashSeq4Hive _)
    val Array(trdate, trpath, tedate, tepath) = args

    val sample = new DNNSample_test(spark, trdate, trpath, tedate, tepath)
    sample.saveTrain()
    //sample.saveTest(gauc = false)
  }

  def getKeys(m: Map[Int, Float]): Int = {
    m.keys.max
  }

  def getFtrlFeature(m: Map[Int, Float]): Seq[Float] = {
    for (i <- 1 to 410) yield m.getOrElse(i, 0f)
  }

  def hashSeq4Hive(values: Seq[String]): Seq[Long] = {
    for (v <- values) yield Murmur3Hash.stringHash64(v, 0)
  }
}


/**
  * 详情页在d3版本特征上增加文章id和文章category
  * created time : 2018/11/10 14:13
  *
  * 只取特定栏位的广告进行训练
  * modified time : 2018/11/21 10:40
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSample_test(spark: SparkSession, trdate: String = "", trpath: String = "",
                     tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private def sql(date: String, adslot_type: Int = 1) =
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
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = $adslot_type
       |  and media_appsid in ("80000001", "80000002")
       |  and adslotid in (7096368,7132208,7034978,7453081,7903746)
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
      """.stripMargin

  //用户历史点击和看过的广告
  private def behavior_sql(date: String) =
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

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 2)
    val behaviorSql = behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    transform2TF(spark, spark.sql(trainSql)
      .join(behavior_data, Seq("uid"), "left")
      .join(userAppIdx, Seq("uid"), "left"))
  }

  override def getTestSample4Gauc(spark: SparkSession, date: String, percent: Double = 0.05): DataFrame = {
    val testSql = sql(date, 2)
    val behaviorSql = behavior_sql(date)

    println("=================PREPARING TEST DATA================")
    println(testSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")

    import spark.implicits._
    val rawTrain = spark.sql(testSql)
    val uid = rawTrain.groupBy("uid").agg(expr("sum(label[0]) as count"))
      .filter("count>0")
      .sample(withReplacement = false, percent)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val rawBehavior = spark.sql(behaviorSql)
    val behavior_data = rawBehavior
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

    transform2TF(
      spark, rawTrain.join(uid, Seq("uid"), "inner")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
    )
  }

  def transform2TF(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    data.select($"label",

      hash("uid")($"uid").alias("sample_idx"),
      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),
      hash("f28")($"hour").alias("f28"),
      hash("f29")($"content_id").alias("f29"),
      hash("f30")($"content_category").alias("f30"),

      array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
        $"m11", $"m12", $"m13", $"m14", $"m15").alias("raw_sparse")
    )
      .select(
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28",
          $"f29", $"f30").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        $"label", $"sample_idx"
      )
      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }
}

/**
  * 列表页d3特征上再增加小时增量展示和点击ideaid
  * created time : 2018/11/10 16:13
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSample_test1(spark: SparkSession, trdate: String = "", trpath: String = "",
                      tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private def sql(date: String, adslot_type: Int) =
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
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = $adslot_type
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
      """.stripMargin

  //用户历史点击和看过的广告
  private def behavior_sql(date: String) =
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

  //用户当天小时增量查看和点击过的广告
  private def r_behavior_sql(date: String) =
    s"""
       |select uid,
       |      collect_set(concat(hour, ':', ext['adclass'].int_value)) as s_adclass,
       |      collect_set(if(isclick = 1, concat(hour, ':', ext['adclass'].int_value), null)) as c_adclass
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = 1
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
       |group by uid
      """.stripMargin

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val r_behaviroSql = r_behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(r_behaviroSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val r_behavior_data = spark.sql(r_behaviroSql)

    transform2TF(spark,
      spark.sql(trainSql)
        .join(r_behavior_data, Seq("uid"), "left")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left"))
  }

  override def getTestSample(spark: SparkSession, date: String, percent: Double): DataFrame = {
    val testSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val r_behaviroSql = r_behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(testSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(r_behaviroSql)
    println("====================================================")

    import spark.implicits._
    val rawTest = spark.sql(testSql)
      .sample(withReplacement = false, 0.03)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val rawBehavior = spark.sql(behaviorSql)
    val behavior_data = rawBehavior
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

    val r_behavior_data = spark.sql(r_behaviroSql)

    transform2TF(
      spark,
      rawTest
        .join(r_behavior_data, Seq("uid"), "left")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
    )
  }

  override def getTestSample4Gauc(spark: SparkSession, date: String, percent: Double = 0.05): DataFrame = {
    val testSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val r_behaviroSql = r_behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(testSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(r_behaviroSql)
    println("====================================================")

    import spark.implicits._
    val rawTest = spark.sql(testSql)
    val uid = rawTest.groupBy("uid").agg(expr("sum(label[0]) as count"))
      .filter("count>0")
      .sample(withReplacement = false, percent)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val rawBehavior = spark.sql(behaviorSql)
    val behavior_data = rawBehavior
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

    val r_behavior_data = spark.sql(r_behaviroSql)

    transform2TF(
      spark,
      rawTest.join(uid, Seq("uid"), "inner")
        .join(r_behavior_data, Seq("uid"), "left")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
    )
  }

  def transform2TF(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    data.select($"label",

      hash("uid")($"uid").alias("sample_idx"),
      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),
      hash("f28")($"hour").alias("f28"),

      array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
        $"m11", $"m12", $"m13", $"m14", $"m15",
        filterHash3($"hour", $"adclass", $"s_adclass").alias("m16"),
        filterHash3($"hour", $"adclass", $"c_adclass").alias("m17")).alias("raw_sparse")
    )
      .select(
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        $"label", $"sample_idx"
      )
      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }
}

/**
  * d4特征与d3特征一致（训练和测试时，增加adclass 5个交叉特征，去除uid），准备gauc测试数据
  * created time : 2018/11/13 17:13
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSample_test2(spark: SparkSession, trdate: String = "", trpath: String = "",
                      tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private def sql(date: String, adslot_type: Int) =
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
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = $adslot_type
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
      """.stripMargin

  //用户历史点击和看过的广告
  private def behavior_sql(date: String) =
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

  //用户当天小时增量查看和点击过的广告
  private def r_behavior_sql(date: String) =
    s"""
       |select uid,
       |      collect_set(concat(hour, ':', hash(concat('m16',ext['adclass'].int_value), 0))) as s_adclass,
       |      collect_set(if(isclick = 1, concat(hour, ':', hash(concat('m17',ext['adclass'].int_value), 0)), null)) as c_adclass
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
       |group by uid
      """.stripMargin

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val r_behaviroSql = r_behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(r_behaviroSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val r_behavior_data = spark.sql(r_behaviroSql)

    transform2TF(spark,
      spark.sql(trainSql)
        .join(r_behavior_data, Seq("uid"), "left")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left"))
  }

  override def getTestSample4Gauc(spark: SparkSession, date: String, percent: Double = 0.05): DataFrame = {
    val testSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val r_behaviroSql = r_behavior_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(testSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(r_behaviroSql)
    println("====================================================")

    import spark.implicits._
    val rawTest = spark.sql(testSql)
    val uid = rawTest.groupBy("uid").agg(expr("sum(label[0]) as count"))
      .filter("count>0")
      .sample(withReplacement = false, percent)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val rawBehavior = spark.sql(behaviorSql)
    val behavior_data = rawBehavior
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

    val r_behavior_data = spark.sql(r_behaviroSql)

    transform2TF(
      spark,
      rawTest.join(uid, Seq("uid"), "inner")
        .join(r_behavior_data, Seq("uid"), "left")
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
    )
  }

  def transform2TF(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    data.select($"label",

      hash("uid")($"uid").alias("sample_idx"),
      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),
      hash("f28")($"hour").alias("f28"),

      array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
        $"m11", $"m12", $"m13", $"m14", $"m15").alias("raw_sparse")
    )
      .select(
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        $"label", $"sample_idx"
      )
      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }
}

/**
  * d4增加用户最近阅读过的类目，作者名，阅读关键词
  * created time : 2018/11/19 17:00
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSample_test3(spark: SparkSession, trdate: String = "", trpath: String = "",
                      tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private def sql(date: String, adslot_type: Int) =
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
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = $adslot_type
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
      """.stripMargin

  //用户历史点击和看过的广告
  private def behavior_sql(date: String) =
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

  //用户阅读过的类目，作者，关键词
  private def rec_info_sql(date: String) =
    s"""
       |select device_id as uid,
       |     features['u_dy_6_readcate'].stringarrayvalue          as cat,
       |     features['u_dy_5_readsourcename'].stringarrayvalue    as author,
       |     features['u_dy_5_readkeyword'].stringarrayvalue       as word
       |from dl_cpc.cpc_user_features_from_algo
       |where load_date='$date'
    """.stripMargin

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val recSql = rec_info_sql(date)
    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(recSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val recData = getRecData(spark, recSql)

    transform2TF(spark,
      spark.sql(trainSql)
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
        .join(recData, Seq("uid"), "left"))
  }


  override def getTestSample(spark: SparkSession, date: String, percent: Double): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val recSql = rec_info_sql(date)
    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(recSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val recData = getRecData(spark, recSql)

    transform2TF(spark,
      spark.sql(trainSql)
        .sample(withReplacement = false, 0.03)
        .join(behavior_data, Seq("uid"), "left")
        .join(userAppIdx, Seq("uid"), "left")
        .join(recData, Seq("uid"), "left"))
  }


  def transform2TF(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    data.select($"label",

      hash("uid")($"uid").alias("sample_idx"),
      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),
      hash("f28")($"hour").alias("f28"),

      array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
        $"m11", $"m12", $"m13", $"m14", $"m15", $"m16").alias("raw_sparse")
    )
      .select(
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        $"label", $"sample_idx"
      )
      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }

  //获取推荐阅读特征
  def getRecData(spark: SparkSession, sql: String): DataFrame = {
    import spark.implicits._
    val data = spark.sql(sql).persist()

    val catHash = data.select(explode($"cat").alias("cat")).distinct().withColumn("m16", hash("m16")($"cat"))
      .rdd.map(x => {
      x.getAs[String]("cat") -> x.getAs[Long]("m16")
    }).collect().toMap
    val catMap = spark.sparkContext.broadcast(catHash)

    /*val authorHash = data.select(explode($"author").alias("author")).distinct().withColumn("m17", hash("m17")($"author"))
      .rdd.map(x => {
      x.getAs[String]("author") -> x.getAs[Long]("m17")
    }).collect().toMap
    val authorMap = spark.sparkContext.broadcast(authorHash)

    val wordHash = data.select(explode($"word").alias("word")).withColumn("m18", hash("m18")($"word"))
      .rdd.map(x => {
      x.getAs[String]("word") -> x.getAs[Long]("m18")
    }).collect().toMap
    val wordMap = spark.sparkContext.broadcast(wordHash)*/

    data.select($"uid",
      findHash(catMap.value, Murmur3Hash.stringHash64("m16", 0))($"cat").alias("m16")
      /*findHash(authorMap.value, Murmur3Hash.stringHash64("m17", 0))($"author").alias("m17"),
      findHash(wordMap.value, Murmur3Hash.stringHash64("m18", 0))($"word").alias("m18")*/
    )
  }
}

/**
  * d3特征增加ftrldense特征
  * created time : 2018/11/20 14:06
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSample_test4(spark: SparkSession, trdate: String = "", trpath: String = "",
                      tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private def sql(date: String, adslot_type: Int = 1) =
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
       |  hour, ext_int['content_id'] as content_id, ext_int['category'] as content_category,
       |
       |  searchid
       |
       |from dl_cpc.cpc_union_log where `date` = '$date'
       |  and isshow = 1 and ideaid > 0 and adslot_type = $adslot_type
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and length(uid) in (14, 15, 36)
      """.stripMargin

  //用户历史点击和看过的广告
  private def behavior_sql(date: String) =
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

  //ftrl的dense特征
  private def ftrl_sql(date: String) =
    s"""
       |select searchid,
       |       getFtrlFeature(feature_vector) as ftrl
       |from dl_cpc.ml_snapshot_from_show
       |where date = '$date'
    """.stripMargin

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val ftrlSql = ftrl_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(ftrlSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val ftrl_data = spark.sql(ftrlSql)

    transform2TF(spark, spark.sql(trainSql)
      .join(ftrl_data, Seq("searchid"), "left")
      .join(behavior_data, Seq("uid"), "left")
      .join(userAppIdx, Seq("uid"), "left"))
  }


  override def getTestSample(spark: SparkSession, date: String, percent: Double = 0.03): DataFrame = {
    import spark.implicits._
    val trainSql = sql(date, 1)
    val behaviorSql = behavior_sql(date)
    val ftrlSql = ftrl_sql(date)

    println("=================PREPARING TRAIN DATA==============")
    println(trainSql)
    println("====================================================")
    println(behaviorSql)
    println("====================================================")
    println(ftrlSql)
    println("====================================================")

    val rawBehavior = spark.sql(behaviorSql)

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val behavior_data = rawBehavior
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

    val ftrl_data = spark.sql(ftrlSql)

    transform2TF(spark, spark.sql(trainSql)
      .sample(withReplacement = false, percent)
      .join(ftrl_data, Seq("searchid"), "left")
      .join(behavior_data, Seq("uid"), "left")
      .join(userAppIdx, Seq("uid"), "left"))
  }

  def transform2TF(spark: SparkSession, data: DataFrame): DataFrame = {
    import spark.implicits._

    val defaultNa = for (i <- 1 to 410) yield 0f

    data.select($"label",

      hash("uid")($"uid").alias("sample_idx"),
      hash("f1")($"media_type").alias("f1"),
      hash("f2")($"mediaid").alias("f2"),
      hash("f3")($"channel").alias("f3"),
      hash("f4")($"sdk_type").alias("f4"),
      hash("f5")($"adslot_type").alias("f5"),
      hash("f6")($"adslotid").alias("f6"),
      hash("f7")($"sex").alias("f7"),
      hash("f8")($"dtu_id").alias("f8"),
      hash("f9")($"adtype").alias("f9"),
      hash("f10")($"interaction").alias("f10"),
      hash("f11")($"bid").alias("f11"),
      hash("f12")($"ideaid").alias("f12"),
      hash("f13")($"unitid").alias("f13"),
      hash("f14")($"planid").alias("f14"),
      hash("f15")($"userid").alias("f15"),
      hash("f16")($"is_new_ad").alias("f16"),
      hash("f17")($"adclass").alias("f17"),
      hash("f18")($"site_id").alias("f18"),
      hash("f19")($"os").alias("f19"),
      hash("f20")($"network").alias("f20"),
      hash("f21")($"phone_price").alias("f21"),
      hash("f22")($"brand").alias("f22"),
      hash("f23")($"province").alias("f23"),
      hash("f24")($"city").alias("f24"),
      hash("f25")($"city_level").alias("f25"),
      hash("f26")($"uid").alias("f26"),
      hash("f27")($"age").alias("f27"),
      hash("f28")($"hour").alias("f28"),

      array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
        $"m11", $"m12", $"m13", $"m14", $"m15").alias("raw_sparse"),

      $"ftrl".alias("float_dense")
    )
      .select(
        array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        $"float_dense",

        $"label", $"sample_idx"
      )
      .select(
        $"sample_idx",
        $"label",
        $"dense",
        $"float_dense",
        $"sparse._1".alias("idx0"),
        $"sparse._2".alias("idx1"),
        $"sparse._3".alias("idx2"),
        $"sparse._4".alias("id_arr")
      )
  }
}


