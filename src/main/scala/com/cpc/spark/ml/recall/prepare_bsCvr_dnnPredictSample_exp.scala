package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, broadcast, udf}
import org.apache.spark.storage.StorageLevel

object prepare_bsCvr_dnnPredictSample_exp {
  Logger.getRootLogger.setLevel(Level.WARN)

  //multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("dnn bsCvr predictSample exp")
      .enableHiveSupport()
      .getOrCreate()
    val date = args(0)
    //    val hour = args(1)

    val predictionSample = getSample(spark, date)//.persist()

    //val n = predictionSample.count()
    //println("训练数据：total = %d".format(n))

    val sampleDay = getDay(date, 1)

    predictionSample//.repartition(6000)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/sample/recall/dnn_recall_cvr_v1/dnnprediction_exp-$sampleDay")

    //train.take(10).foreach(println)

    predictionSample.unpersist()
  }

  def getSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    val day = getDay(date, 1)
    val dayFeature = getDay(date, 1)

    val behavior_data = spark.read.parquet(s"hdfs://emr-cluster/user/cpc/features/adBehaviorFeature/$dayFeature")

    val uidRequest = spark.read.parquet("hdfs://emr-cluster/user/cpc/features/timeDistributionFeature").
      select($"uid", hashSeq("m26", "string")($"request").alias("m26"))

    val profileData = spark.read.parquet("hdfs://emr-cluster/user/cpc/qtt-lookalike-sample/pv1").
      select($"did".alias("uid"), hashSeq("m1", "string")($"apps._1").alias("m1"),
        hashSeq("m24", "string")($"words").alias("m24"),
        hashSeq("m25", "string")($"terms").alias("m25")
      )

    //连接adv后台，从mysql中获取ideaid的相关信息
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    //从adv后台mysql获取人群包的url
    /**
    val table="(select user_id as userid, adslot_type, type as adtype, clk_site_id as site_id, category as adclass from adv.idea where status=0 and audit=1) as tmp"
    val idea = spark.read.jdbc(jdbcUrl, table, jdbcProp).distinct()
    idea.printSchema()

    idea.show(5)
      */
    val precision=
      s"""
        |(select id as unitid, audience_orient,precition_tag from adv.unit ta
        |left join (select user_id,look_like_id as precition_tag from adv.look_like where type=2 and status=0 group by user_id) tb on ta.user_id=tb.user_id
        |where audience_orient>0) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, precision, jdbcProp).createOrReplaceTempView("precision")

    spark.sql(
      s"""
         |select distinct unitid from precision lateral view explode(split(audience_orient,',')) audience_orient as tag
         |where tag in (select distinct precition_tag from precision where precition_tag is not null) or tag in ('297')
       """.stripMargin
    ).createOrReplaceTempView("precision_unit")

    val adv=
      s"""
        |(select id as unitid, tb.user_id as userid, plan_id as planid, adslot_type, charge_type, cnt,
        |REPLACE(tb.os_type,'0','0,1,2,3') as os_type1,
        |REPLACE(tb.age,'0','0,1,2,3,4') as age,
        |case when tb.sex=0 then '0,1,2' when tb.sex=1 then '1' else '2' end as sex,
        |case when tb.regions>0 then tb.regions else '0' end as regions from
        |(SELECT unit_id,SUM(cost) as cnt FROM adv.cost where cost>0 and date>='$day' group by unit_id) ta
        |join adv.unit tb on ta.unit_id=tb.id
        |where tb.audience_orient>0) temp
      """.stripMargin

    spark.read.jdbc(jdbcUrl, adv, jdbcProp).createOrReplaceTempView("adv")
    val table2=
      s"""
         |select ta.unitid,ta.userid,ta.planid,ta.adslot_type,ta.charge_type, os_type, age1, sex1, regions1
         |from (select unitid, userid, planid, adslot_type, charge_type, cnt, os_type, age1, sex1, regions1 from adv
         |lateral view explode(split(os_type1,',')) os_type1 as os_type
         |lateral view explode(split(age,',')) age as age1
         |lateral view explode(split(sex,',')) sex as sex1
         |lateral view explode(split(regions,',')) regions as regions1 where unitid
         |not in (select unitid from precision_unit) and unitid not in (select unitid from dl_cpc.cpc_recall_high_confidence_unitid where date>'$day' group by unitid)) ta join
         |(select unitid from dl_cpc.cpc_id_bscvr_auc where tag='unitid' and day='$day' and label=1 group by unitid) tb
         |on ta.unitid=tb.unitid
         |join (select unitid from dl_cpc.cpc_recall_unitid_ctr_dif where dt='$day' and ratio<1.5 group by unitid) tc
         |on ta.unitid=tc.unitid
         |where ta.cnt>50000
         |""".stripMargin


    spark.sql(table2).select("unitid").createTempView("unitid_table")

    val table3=
      s"""insert overwrite table dl_cpc.cpc_recall_bsExp_unitid_exp partition (`date`='$day')
         |select unitid from unitid_table
      """.stripMargin
    spark.sql(table3)

    val unit_info = spark.sql(table2)
    unit_info.count

    val unit_hash = unit_info.select(hash("f1")($"adslot_type").alias("f1"),
      //hash("f6")($"adslotid").alias("f6"),
      //hash("f2")($"sex").alias("f2"),
      //hash("f8")($"dtu_id").alias("f8"),
      hash("f3")($"planid").alias("f3"),
      //hash("f10")($"interaction").alias("f10"),
      //hash("f11")($"bid").alias("f11"),
      //hash("f4")($"ideaid").alias("f4"),
      hash("f4")($"unitid").alias("f4"),
      //("f6")($"planid").alias("f6"),
      hash("f5")($"userid").alias("f5"),
      //hash("f16")($"is_new_ad").alias("f16"),
      hash("f6")($"charge_type").alias("f6")
      ,$"unitid", $"age1", $"sex1", $"regions1", $"os_type")
    unit_hash.show(10)

    val sql =
      s"""
         |select
         |  max(os) as os, max(phone_price) as phone_price,
         |  max(brand_title) as brand,
         |  max(province) as province, max(city) as city, max(city_level) as city_level,
         |  uid, max(age) as age, max(sex) as sex
         |from dl_cpc.cpc_basedata_union_events where day = '$day'
         |  and ideaid > 0
         |  and media_appsid in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and uid is not null group by uid
      """.stripMargin
    println("--------------------------------")
    println(sql)
    println("--------------------------------")

    val result_temp =
      spark.sql(sql)
        .join(profileData, Seq("uid"), "leftouter")
        .join(uidRequest, Seq("uid"), "leftouter")
        .join(behavior_data, Seq("uid"), "leftouter")
        .select($"sex",$"age",$"os", $"city", $"province",
          //hash("f1")($"media_type").alias("f1"),
          //hash("f2")($"mediaid").alias("f2"),
          //hash("f3")($"channel").alias("f3"),
          //hash("f4")($"sdk_type").alias("f4"),
          //hash("f1")($"adslot_type").alias("f1"),
          //hash("f6")($"adslotid").alias("f6"),
          hash("f2")($"sex").alias("f2"),
          //hash("f8")($"dtu_id").alias("f8"),
          //hash("f3")($"adtype").alias("f3"),
          //hash("f10")($"interaction").alias("f10"),
          //hash("f11")($"bid").alias("f11"),
          //hash("f4")($"ideaid").alias("f4"),
          //hash("f5")($"unitid").alias("f5"),
          //hash("f6")($"planid").alias("f6"),
          //hash("f7")($"userid").alias("f7"),
          //hash("f16")($"is_new_ad").alias("f16"),
          //hash("f8")($"adclass").alias("f8"),
          //hash("f9")($"site_id").alias("f9"),
          hash("f7")($"os").alias("f7"),
          hash("f8")($"phone_price").alias("f8"),
          //hash("f20")($"network").alias("f20"),
          hash("f9")($"brand").alias("f9"),
          hash("f10")($"province").alias("f10"),
          hash("f11")($"city").alias("f11"),
          hash("f12")($"city_level").alias("f12"),
          hash("f13")($"uid").alias("f13"),
          hash("f14")($"age").alias("f14"),
          //hash("f28")($"hour").alias("f28"),

          mkSparseFeature_m(array($"m1", $"m2", $"m3", $"m4", $"m5", $"m6", $"m7", $"m8", $"m9", $"m10",
            $"m11", $"m12", $"m13", $"m14", $"m15",$"m16", $"m17", $"m18", $"m19", $"m20",$"m21", $"m22",$"m23", $"m24",$"m25",$"m26"))
            .alias("sparse"), $"uid"
        ).select(
        $"f2", $"f7", $"f8", $"f9", $"f10", $"f11", $"f12", $"f13", $"f14",
        $"sparse".getField("_1").alias("idx0"),
        $"sparse".getField("_2").alias("idx1"),
        $"sparse".getField("_3").alias("idx2"),
        $"sparse".getField("_4").alias("id_arr"),
        $"uid",$"sex",$"age",$"os", $"city", $"province"
      ).repartition(8000).persist(StorageLevel.DISK_ONLY)

    result_temp.show(10)

    val bunit_hash = broadcast(unit_hash).persist(StorageLevel.DISK_ONLY)
    bunit_hash.show(10)

    val result_temp1 = result_temp.join(bunit_hash,
      $"sex"===$"sex1" and $"age"===$"age1" and $"os"===$"os_type"
        and ($"city"===$"regions1" or $"province"===$"regions1" or $"regions1"===0)
    ).select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
      $"f10", $"f11", $"f12", $"f13", $"f14").alias("dense"),
      //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
      //mkSparseFeature1($"m1").alias("sparse"), $"label"
      $"idx0",
      $"idx1",
      $"idx2",
      $"id_arr",
      $"uid", $"unitid"
    )//.persist(StorageLevel.DISK_ONLY)

    //result_temp1.show(10)

    //result_temp1.unpersist()
    //ideaid_hash.unpersist()

    result_temp1.rdd.zipWithUniqueId()
      .map { x =>
        (x._2, x._1.getAs[Seq[Long]]("dense"),
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"),
          x._1.getAs[String]("uid"), x._1.getAs[Long]("unitid"))
      }
      .toDF("sample_idx", "dense", "idx0", "idx1", "idx2", "id_arr", "uid", "unitid")

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
      s"""
         |select * from dl_cpc.cpc_user_installed_apps where `load_date` = date_add('$date', -1)
      """.stripMargin).rdd
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

}
