package com.cpc.spark.ml.dnn

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 在d3版本特征的基础上实时增加当天看过和点击过的广告ideaid
  * created time : 2018/11/10 10:35
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNSampleV4_test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val Array(trdate, trpath, tedate, tepath) = args

    val sample = new DNNSampleV4_test(spark, trdate, trpath, tedate, tepath)
    sample.saveTrain()
    sample.saveTest()
  }
}


/**
  *
  * created time : 2018/11/8 14:13
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSampleV4_test(spark: SparkSession, trdate: String = "", trpath: String = "",
                       tedate: String = "", tepath: String = "")
  extends DNNSample(spark, trdate, trpath, tedate, tepath) {

  //union log 取数
  private val sql =
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
       |from dl_cpc.cpc_union_log where `date` = 'XXXX-XX-XX'
       |  and isshow = 1 and ideaid > 0 and adslot_type = 1
       |  and media_appsid in ("80000001", "80000002")
       |  and uid not like "%.%"
       |  and uid not like "%000000%"
       |  and uid > 0
      """.stripMargin

  //用户历史点击和看过的广告
  private val behavior_sql =
    s"""
       |select uid,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 1)}',show_ideaid,null)) as s_ideaid_1,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 1)}',show_adclass,null)) as s_adclass_1,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 2)}',show_ideaid,null)) as s_ideaid_2,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 2)}',show_adclass,null)) as s_adclass_2,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 3)}',show_ideaid,null)) as s_ideaid_3,
       |       collect_set(if(load_date='${getDay("XXXX-XX-XX", 3)}',show_adclass,null)) as s_adclass_3,
       |
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 1)}',click_ideaid,null)) as c_ideaid_1,
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 1)}',click_adclass,null)) as c_adclass_1,
       |
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 2)}',click_ideaid,null)) as c_ideaid_2,
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 2)}',click_adclass,null)) as c_adclass_2,
       |
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 3)}',click_ideaid,null)) as c_ideaid_3,
       |       collect_list(if(load_date='${getDay("XXXX-XX-XX", 3)}',click_adclass,null)) as c_adclass_3,
       |
       |       collect_list(if(load_date>='${getDay("XXXX-XX-XX", 7)}'
       |                  and load_date<='${getDay("XXXX-XX-XX", 4)}',click_ideaid,null)) as c_ideaid_4_7,
       |       collect_list(if(load_date>='${getDay("XXXX-XX-XX", 7)}'
       |                  and load_date<='${getDay("XXXX-XX-XX", 4)}',click_adclass,null)) as c_adclass_4_7
       |from dl_cpc.cpc_user_behaviors
       |where load_date in ('${getDays("XXXX-XX-XX", 1, 7)}')
       |group by uid
      """.stripMargin

  override def getTrainSample(spark: SparkSession, date: String): DataFrame = {
    val rawTrain = spark.sql(sql.replace("XXXX-XX-XX", date))
    spark.sql(sql.replace("XXXX-XX-XX", date))
  }

  override def getTestSamle(spark: SparkSession, date: String, percent: Double): DataFrame = super.getTestSamle(spark, date, percent)

  override def getTestSamle4Gauc(spark: SparkSession, date: String, percent: Double): DataFrame = super.getTestSamle4Gauc(spark, date, percent)
}
