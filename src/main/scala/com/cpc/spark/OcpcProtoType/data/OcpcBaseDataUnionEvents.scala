package com.cpc.spark.OcpcProtoType.data

import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * @author Jinbao
  * @date 2019/3/28 15:37
  */
object OcpcBaseDataUnionEvents {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"OcpcBaseDataUnionEvents date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val baseSql =
            s"""
               |select *
               |from dl_cpc.ocpc_base_unionlog
               |where `date` = '$date' and hour = '$hour'
             """.stripMargin

        val baseData = spark.sql(baseSql)
          .withColumn("ocpc_log_length", udfGetLength()(col("ocpc_log")))   //获取ocpc_log的长度
          .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))   //解析ocpc_log
          .withColumn("media", udfGetMedia()(col("media_appsid")))          //根据media_appsid判断media
          .withColumn("industy", udfGetIndusty()(col("adclass"),col("adslot_type")))

        baseData.createOrReplaceTempView("base_data")

        val sql =
            s"""
               |select
               |    searchid,
               |    timestamp,
               |    network,
               |    exptags,
               |    media_type,
               |    media_appsid,
               |    adslotid,
               |    adslot_type,
               |    adtype,
               |    adsrc,
               |    interaction,
               |    bid,
               |    price,
               |    ideaid,
               |    unitid,
               |    planid,
               |    country,
               |    province,
               |    city,
               |    uid,
               |    ua,
               |    os,
               |    sex,
               |    age,
               |    isshow,
               |    isclick,
               |    duration,
               |    userid,
               |    is_ocpc,
               |    user_city,
               |    city_level,
               |    adclass,
               |    ocpc_log_length,
               |    ocpc_log_dict,
               |    exp_ctr,
               |    exp_cvr,
               |    antispam,
               |    conversion_goal,
               |    charge_type,
               |    conversion_from,
               |    is_api_callback,
               |    siteid,
               |    `date`,
               |    hour,
               |    media,
               |    industy
               |from
               |    base_data
             """.stripMargin

        val result = spark.sql(sql)

        result.printSchema()

        result.repartition(100)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.ocpc_basedata_union_events")

        println(s"insert into dl_cpc.ocpc_basedata_union_events , date = $date , hour = $hour")
    }
}
