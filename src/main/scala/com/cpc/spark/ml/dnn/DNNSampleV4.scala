package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, udf}


/**
  * 增加用户历史兴趣adclass hit-sum
  * 对比v3
  * 修改multihot特征 1、2、3、4-7天点击变成1、2-7
  * 增加onehot特征1、2-7对应adclass命中
  * created time : 2018/11/6 10:50
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNSampleV4 {

  //multihot 默认hash
  private val default_hash = for (i <- 1 to 50) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn sample")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)
    val tdate = args(1)

    getSample(spark, date, is_train = true)
      .repartition(1000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/daily_v4_list/dnntrain-" + date)

    val test = getSample(spark, tdate, is_train = false).persist()
    val tn = test.count
    println("测试数据：total = %d, 正比例 = %.4f".format(tn, test.where("label=array(1,0)").count.toDouble / tn))

    test.repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/zhj/daily_v4_list/dnntest-" + tdate)

    test.take(10).foreach(println)
  }

  def getSample(spark: SparkSession, date: String, is_train: Boolean): DataFrame = {
    import spark.implicits._

    val behavior_sql =
      s"""
         |select uid,
         |       collect_list(if(load_date='${getDay(date, 1)}',click_ideaid,null)) as c_ideaid_1,
         |       collect_list(if(load_date='${getDay(date, 1)}',click_adclass,null)) as c_adclass_1,
         |
         |       collect_list(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 1)}',click_ideaid,null)) as c_ideaid_2_7,
         |       collect_list(if(load_date>='${getDay(date, 7)}'
         |                  and load_date<='${getDay(date, 1)}',click_adclass,null)) as c_adclass_2_7
         |from dl_cpc.cpc_user_behaviors
         |where load_date in ('${getDays(date, 1, 7)}')
         |group by uid
      """.stripMargin

    println("--------------------------------")
    println(behavior_sql)
    println("--------------------------------")

    val raw_behavior = spark.sql(behavior_sql)
      .select(
        $"uid",
        mkCount($"c_ideaid_1").alias("c1_ideaid_count"),
        mkCount($"c_adclass_1").alias("c1_adclass_count"),
        mkCount($"c_ideaid_2_7").alias("c27_ideaid_count"),
        mkCount($"c_adclass_2_7").alias("c27_adclass_count")
      ).persist()

    raw_behavior.show(10)

    val behavior_data = raw_behavior
      .select(
        $"uid",
        hashSeq("m2", "int")(getKeys($"c1_ideaid_count")).alias("m2"),
        hashSeq("m3", "int")(getKeys($"c1_adclass_count")).alias("m3"),
        hashSeq("m4", "int")(getKeys($"c27_ideaid_count")).alias("m4"),
        hashSeq("m5", "int")(getKeys($"c27_adclass_count")).alias("m5"),
        $"c1_ideaid_count",
        $"c1_adclass_count",
        $"c27_ideaid_count",
        $"c27_adclass_count",
        getValues($"c1_ideaid_count").alias("cv1"),
        getValues($"c1_adclass_count").alias("cv2"),
        getValues($"c27_ideaid_count").alias("cv3"),
        getValues($"c27_adclass_count").alias("cv4")
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
         |from dl_cpc.cpc_union_log where `date` = '$date' and hour=10
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

      getHashValue(29)($"ideaid", $"c1_ideaid_count").alias("f29"),
      getHashValue(30)($"adclass", $"c1_adclass_count").alias("f30"),
      getHashValue(31)($"ideaid", $"c27_ideaid_count").alias("f31"),
      getHashValue(32)($"adclass", $"c27_adclass_count").alias("f32"),

      array($"m1", $"m2", $"m3", $"m4", $"m5").alias("raw_sparse"),

      getFloatValue($"ideaid", $"c1_ideaid_count").alias("c1"),
      getFloatValue($"adclass", $"c1_adclass_count").alias("c2"),
      getFloatValue($"ideaid", $"c27_ideaid_count").alias("c3"),
      getFloatValue($"adclass", $"c27_adclass_count").alias("c4"),

      array($"cv1", $"cv2", $"cv3", $"cv4").alias("float_sparse")
    )

      .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
        $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
        $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28", $"f29",
        $"f30", $"f31", $"f32").alias("dense"),

        mkSparseFeature_m($"raw_sparse").alias("sparse"),

        array($"c1", $"c2", $"c3", $"c4").alias("f_dense"),
        mkFloatSparseFeature_m($"float_sparse").alias("f_sparse"),

        $"label"
      )

      .select(
        $"label",
        $"dense",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr"),

        $"f_dense",
        $"f_sparse._1".alias("f_idx0"),
        $"f_sparse._2".alias("f_idx1"),
        $"f_sparse._3".alias("f_idx2"),
        $"f_sparse._4".alias("f_id_arr")
      )

      .rdd.zipWithUniqueId()
      .map { x =>
        (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"),
          x._1.getAs[Seq[Double]]("f_dense"),
          x._1.getAs[Seq[Int]]("f_idx0"), x._1.getAs[Seq[Int]]("f_idx1"),
          x._1.getAs[Seq[Int]]("f_idx2"), x._1.getAs[Seq[Double]]("f_id_arr"))
      }
      .toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr",
        "f_dense", "f_idx0", "f_idx1", "f_idx2", "f_id_arr")
  }

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

  private def mkFloatSparseFeature_m = udf {
    features: Seq[Seq[Double]] =>
      var i = 0
      var re = Seq[(Int, Int, Double)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null && feature.nonEmpty) feature.zipWithIndex.map(x => (i, x._2, x._1)) else Seq((i, 0, 0.0)))
        i = i + 1
      }
      val c = re.map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

  private def mkCount = udf {
    arr: Seq[Int] =>
      var map = Map[Int, Int]()
      for (v <- arr) {
        map = map ++ Map(v -> (map.getOrElse(v, 0) + 1))
      }
      (for (m <- map) yield (m._1, Math.log(m._2 + 1.0))).slice(0, 1000)
  }

  private def getKeys = udf {
    m: Map[Int, Double] => m.keys.toSeq
  }

  private def getValues = udf {
    m: Map[Int, Double] => m.values.toSeq
  }

  private def getHashValue(idx: Int) = udf {
    (v: Int, m: Map[Int, Double]) =>
      if (m.nonEmpty && m.contains(v)) Murmur3Hash.stringHash64("f" + idx + v, 0)
      else Murmur3Hash.stringHash64("f" + idx, 0)
  }

  private def getFloatValue = udf {
    (v: Int, m: Map[Int, Double]) => m.getOrElse(v, 0.0)
  }
}
