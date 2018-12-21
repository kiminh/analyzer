package com.cpc.spark.ml.dnn.cvr

import com.cpc.spark.common.Murmur3Hash
import com.cpc.spark.ml.dnn.DNNSample
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 二类电商cvr
  * created time : 2018/12/05 15:00
  *
  * @author zhj
  * @version 1.0
  *
  */
object CvrSampleV4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(trdate, trpath, tedate, tepath) = args

    val sample = new CvrSampleV4(spark, trdate, trpath, tedate, tepath)
    sample.saveTrain()
  }
}

class CvrSampleV4(spark: SparkSession, trdate: String = "", trpath: String = "",
                  tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  private def getAsFeature(date: String, adtype: Int = 1): DataFrame = {
    import spark.implicits._
    val raw_data_path = s"/user/cpc/dnn/raw_data_list/$date"
    println("============= as features ==============")
    println(raw_data_path)

    spark.read.parquet(raw_data_path)
      .select($"cvr_label".alias("label"),
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

  private def getUdFeature(date: String): DataFrame = {
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
        hashSeq("ud14#", "int")($"c_adclass_4_7").alias("ud14")
      )
  }

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._

    var data: DataFrame = null

    if (date.length == 10) {
      data = getAsFeature(date)
        .join(getUdFeature(date), Seq("uid"), "left")
      /*.join(broadcast(getAdFeature(date)), Seq("ideaid"), "left")
  }
  else if (date.length == 13) {
    val dt = date.substring(0, 10)
    val h = date.substring(11, 13).toInt
    data = getAsFeature_hourly(dt, h)
      .join(getUdFeature_hourly(date), Seq("uid"), "left")
      .join(broadcast(getAdFeature_hourly(date)), Seq("ideaid"), "left")
      .persist()*/
    } else {
      println(date)
      println("-------------------日期格式传入错误-----------------")
      println("       正确格式：yyyy-MM-dd 或 yyyy-MM-dd-HH        ")
      println("---------------------------------------------------")
      sys.exit(1)
    }


    val columns = Seq("ud0", "ud1", "ud2", "ud3", "ud4", "ud5", "ud6", "ud7", "ud8", "ud9", "ud10",
      "ud11", "ud12", "ud13", "ud14")
    val default_hash = for (col <- columns.zipWithIndex)
      yield (col._2, 0, Murmur3Hash.stringHash64(col._1 + "#", 0))

    data
      .select(
        $"label",
        $"uid",
        $"dense",
        mkSparseFeature(default_hash)(
          array($"ud0", $"ud1", $"ud2", $"ud3", $"ud4", $"ud5", $"ud6", $"ud7", $"ud8"
            , $"ud9", $"ud10", $"ud11", $"ud12", $"ud13", $"ud14")
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
