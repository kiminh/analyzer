package com.cpc.spark.ml.dnn.trash

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{array, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * dnn ctr v3   共28个dense，37个multi hot特征，
  * 新增24个multi hot特征
  * created time : 2018/10/31 11:01
  *
  * @author zhj
  * @version 1.0
  *
  */
@deprecated
object DNNSampleV3 {

  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val date = args(0)
    val tdate = args(1)

    val default_hash_uid = Murmur3Hash.stringHash64("f26", 0)

    val rawtrain = getSample(spark, date, is_train = true).withColumn("uid", $"dense" (25))

    rawtrain.printSchema()

    val uid = rawtrain.select("uid")
      .groupBy("uid").count()

    val train = rawtrain.join(uid, Seq("uid"), "left")
      .select($"sample_idx", $"label",
        getNewDense(25, default_hash_uid)($"dense", $"count" < 4).alias("dense"),
        $"idx0", $"idx1", $"idx2", $"id_arr")

    /*val n = train.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(n, train.where("label=array(1,0)").count.toDouble / n))*/

    train.repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/daily_v3_list/dnntrain-" + date)

    val test = getSample(spark, tdate, is_train = false).persist()
    val tn = test.count
    println("测试数据：total = %d, 正比例 = %.4f".format(tn, test.where("label=array(1,0)").count.toDouble / tn))

    test.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/daily_v3_list/dnntest-" + tdate)
    test.take(10).foreach(println)
  }

  /*  def getSample(spark: SparkSession, date: String, is_train: Boolean): DataFrame = {
      import spark.implicits._

      val behavior_sql =
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
           |       collect_set(if(load_date='${getDay(date, 2)}',click_brand_title,null)) as c_brand_title_2,
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
        """.stripMargin

      println("--------------------------------")
      println(behavior_sql)
      println("--------------------------------")

      val behavior_data = spark.sql(behavior_sql)
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

      val userAppIdx = getUidApp(spark, date)
        .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

      val sql =
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
           |  hour
           |
           |from dl_cpc.cpc_union_log where `date` = '$date'
           |  and isshow = 1 and ideaid > 0 and adslot_type = 1
           |  and media_appsid in ("80000001", "80000002")
           |  and uid not like "%.%"
           |  and uid not like "%000000%"
           |  and uid > 0
        """.stripMargin
      println("--------------------------------")
      println(sql)
      println("--------------------------------")

      val re =
        if (is_train)
          spark.sql(sql)
            .join(userAppIdx, Seq("uid"), "leftouter")
            .join(behavior_data, Seq("uid"), "leftouter")
        else
          spark.sql(sql)
            .join(userAppIdx, Seq("uid"), "leftouter")
            .join(behavior_data, Seq("uid"), "leftouter")
            .sample(withReplacement = false, 0.03)

      re.select($"label",

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
          $"m11", $"m12", $"m13", $"m14", $"m15", $"m16", $"m17", $"m18", $"m19", $"m20",
          $"m21", $"m22", $"m23", $"m24", $"m25", $"m26", $"m27", $"m28", $"m29", $"m30",
          $"m31", $"m32", $"m33", $"m34", $"m35", $"m36", $"m37").alias("raw_sparse")
      )

        .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
          $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
          $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),
          //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
          //mkSparseFeature1($"m1").alias("sparse"), $"label"
          mkSparseFeature_m($"raw_sparse").alias("sparse"),
          $"label"
        )

        .select(
          $"label",
          $"dense",
          $"sparse".getField("_1").alias("idx0"),
          $"sparse".getField("_2").alias("idx1"),
          $"sparse".getField("_3").alias("idx2"),
          $"sparse".getField("_4").alias("id_arr")
        )

        .rdd.zipWithUniqueId()
        .map { x =>
          (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
            x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
            x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
        }
        .toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
    }*/

  def getSample(spark: SparkSession, date: String, is_train: Boolean): DataFrame = {
    import spark.implicits._

    val behavior_sql =
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
         |from dl_cpc.cpc_user_behaviors
         |where load_date in ('${getDays(date, 1, 7)}')
         |    and rn <= 1000
         |group by uid
      """.stripMargin

    println("--------------------------------")
    println(behavior_sql)
    println("--------------------------------")

    val behavior_data = spark.sql(behavior_sql)
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

        hashSeq("m14", "int")($"c_ideaid_4_7").alias("m14"),
        hashSeq("m15", "int")($"c_adclass_4_7").alias("m15")

      )

    val userAppIdx = getUidApp(spark, date)
      .select($"uid", hashSeq("m1", "string")($"pkgs").alias("m1"))

    val sql =
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
         |  hour
         |
         |from dl_cpc.cpc_union_log where `date` = '$date'
         |  and isshow = 1 and ideaid > 0 and adslot_type = 1
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    val re =
      if (is_train)
        spark.sql(sql)
          .join(userAppIdx, Seq("uid"), "leftouter")
          .join(behavior_data, Seq("uid"), "leftouter")
      else
        spark.sql(sql)
          .join(userAppIdx, Seq("uid"), "leftouter")
          .join(behavior_data, Seq("uid"), "leftouter")
          .sample(withReplacement = false, 0.03)

    re.select($"label",

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

      .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
        $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
        $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28").alias("dense"),
        //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
        //mkSparseFeature1($"m1").alias("sparse"), $"label"
        mkSparseFeature_m($"raw_sparse").alias("sparse"),
        $"label"
      )

      .select(
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr")
      )

      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
      }
      .toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
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
    for (_ <- 1 until day2) {
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

  def getUidApp(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    spark.sql(
      """
        |select * from dl_cpc.cpc_user_installed_apps where `load_date` = "%s"
      """.stripMargin.format(date)).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs")
  }

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @return
    */
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash64(prefix + num, 0) else Murmur3Hash.stringHash64(prefix, 0)
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

  /**
    * 更具指定条件使用默认值更换dense特征中的值
    *
    * @param p :位置 0 ~ length-1
    * @param d :默认值
    * @return
    */
  private def getNewDense(p: Int, d: Long) = udf {
    (dense: Seq[Long], f: Boolean) =>
      if (f) (dense.slice(0, p) :+ d) ++ dense.slice(p + 1, 1000) else dense
  }

  private def mkSparseFeature_m = udf {
    features: Seq[Seq[Long]] =>
      var i = 0
      var re = Seq[(Int, Int, Long)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null) feature.zipWithIndex.map(x => (i, x._2, x._1)) else default_hash(i))
        i = i + 1
      }
      val c = re.map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }


}
