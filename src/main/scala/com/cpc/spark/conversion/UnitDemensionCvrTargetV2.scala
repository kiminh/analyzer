package com.cpc.spark.conversion

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * wangjun
  * desc:广告单元维度转化目标表
  */
object UnitDemensionCvrTargetV2 {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val dbName = args(1)
    println("day: " + date)

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("unit demension conversion target from trident [%s]".format(date))
      .enableHiveSupport()
      .getOrCreate()


    val config = ConfigFactory.load("ocpc")
    val advProps = new Properties()
    var advUrl = config.getString("adv_read_mysql.new_deploy.url")
    advProps.put("user", config.getString("adv_read_mysql.new_deploy.user"))
    advProps.put("password", config.getString("adv_read_mysql.new_deploy.password"))
    advProps.put("driver", config.getString("adv_read_mysql.new_deploy.driver"))
    println(advUrl)


    //user
    val userSql = "(select id as userid, category as adclass from user) as t"
    val users = spark.read.jdbc(advUrl, userSql, advProps).select(
      col("userid").cast("int"),
      col("adclass").cast("int")
    )

    //plan
    val planSql = "(select id as planid, user_id as userid, action_type as interaction from plan) as t"
    val plans = spark.read.jdbc(advUrl, planSql, advProps).select(
      col("planid").cast("int"),
      col("userid").cast("int"),
      col("interaction").cast("int")
    )

    //unit
    import spark.implicits._
    val unitSql = "(select id as unitid, ideas, plan_id as planid, is_ocpc, adslot_type, cast(conversion_goal as char) as conversion_goal, target_medias from unit) as t"
    val units = spark.read.jdbc(advUrl, unitSql, advProps).flatMap { r =>
      var unit = Set[(Int, Int, Int, Int, String, Int, Int, String)]()
      val unitid = r.getAs[Long]("unitid").toInt
      val planid = r.getAs[Long]("planid").toInt
      val is_ocpc = if (r.getAs[Boolean]("is_ocpc") == true) 1 else 0
      val adslot_type = r.getAs[Int]("adslot_type")
      val ideass = r.getAs[String]("ideas").trim
      val ideaArr = r.getAs[String]("ideas").trim.split(",")
      val conversionGoal = r.getAs[String]("conversion_goal").toInt
      val targetMedias = r.getAs[String]("target_medias")
      for (i <- 0 until (ideaArr.length)) {
        val tuple = (planid, unitid, is_ocpc, adslot_type, ideass, (if (ideaArr(i) == "") 0 else ideaArr(i).trim.toInt), conversionGoal, targetMedias)
        unit += tuple
      }
      unit
    }.toDF("planid", "unitid", "is_ocpc", "adslot_type", "ideas", "ideaid", "conversion_goal", "target_medias")

    //idea 取审核通过或待审核的
    val ideaSql = "(select id as ideaid, clk_site_id as siteid, click_feedback_url, target_url, audit from idea) as t"
    val ideas = spark.read.jdbc(advUrl, ideaSql, advProps).select(
      col("ideaid").cast("int"),
      col("siteid").cast("int"),
      col("audit").cast("int"),
      col("click_feedback_url").cast("string"),
      col("target_url").cast("string")
    )

    //site template, chitu have not base_template
    val siteSql = "(select id as site_id, base_template from site_building) as t"
    val sites = spark.read.jdbc(advUrl, siteSql, advProps).select(
      col("site_id").cast("int"),
      col("base_template").cast("string")
    )

    val union = users.join(plans, Seq("userid")).join(units, Seq("planid")).join(ideas, Seq("ideaid"), "left_outer").select(
      col("userid"),
      col("adclass"),
      col("planid"),
      col("interaction"),
      col("unitid"),
      col("is_ocpc"),
      col("adslot_type"),
      col("ideas"),
      col("ideaid"),
      col("conversion_goal"),
      col("target_medias"),
      expr("if(siteid is null, -1, siteid)").alias("siteid"),
      expr("if(audit is null, 2, audit)").alias("audit"), //audit为null时, 设为拒审
      expr("case when click_feedback_url like '%__CALLBACK_URL__%' or target_url like '%__CALLBACK_URL__%' then 1 else 0 end").alias("is_api_callback")//标记是否为api回传
    ).cache

    //取出is_api_callback=1的，即所有api的unitid
    val api_unitid = union.where("is_api_callback=1").select(col("unitid").alias("unit_id")).distinct()

    //通过api的unitid 取出所有的none-api数据
    val non_api_units = union.join(api_unitid, union("unitid") === api_unitid("unit_id"), "left_outer").where("unit_id is null")

    //1. API 通过non-api的unitid 取出所有的api数据
    val api_units = union.join(api_unitid, union("unitid") === api_unitid("unit_id"))
      .drop("ideaid", "siteid", "is_api_callback", "unit_id", "audit")
      .distinct()
      .withColumn("conversion_target", lit(Array[String]("api")))
    println("api_units: " + api_units.count())


    //2.1 Non-API 应用下载
    val non_api_appdown_units = non_api_units.where("interaction=0")
      .drop("ideaid", "siteid", "is_api_callback", "unit_id", "audit")
      .distinct()
      .withColumn("conversion_target", lit(Array[String]("sdk_app_install")))
    println("non_api_appdown_units: " + non_api_appdown_units.count())

    //2.2 Non-API 落地页访问
    val non_api_ldy = non_api_units.where("interaction=1")

    //2.2.1 Non-API 用户行业为应用下载
    val non_api_ldy_appdown_units = non_api_ldy.where(expr("substr(adclass,1,3)=100"))
      .drop("ideaid", "siteid", "is_api_callback", "unit_id", "audit")
      .distinct()
      .withColumn("conversion_target", lit(Array[String]("sdk_app_install")))
    println("non_api_ldy_appdown_units: " + non_api_ldy_appdown_units.count())

    //2.2.2 Non-API 用户行业为电商购物
    val non_api_ldy_dsgw_units = non_api_ldy.where(expr("substr(adclass,1,3)=134"))
      .drop("ideaid", "siteid", "is_api_callback", "unit_id", "audit")
      .distinct()
      .withColumn("conversion_target", lit(Array[String]("site_form")))
    println("non_api_ldy_dsgw_units: " + non_api_ldy_dsgw_units.count())

    //2.2.3 Non-API 非应用下载行业非电商购物
    val non_api_ldy_non_appdown = non_api_ldy.where(expr("substr(adclass,1,3)!=100 and substr(adclass,1,3)!=134")).drop("unit_id")
    //Non-API 非应用下载行业 所有建站unit_id
    val non_api_ldy_non_appdown_site_unitid = non_api_ldy_non_appdown.where("siteid>0 and audit<=1").select(col("unitid").alias("unit_id")).distinct()

    //2.2.3.1 通过非应用下载行业 所有建站unit_id取出所有非建站数据
    val non_api_ldy_non_appdown_none_site_units = non_api_ldy_non_appdown
      .join(non_api_ldy_non_appdown_site_unitid, non_api_ldy_non_appdown("unitid") === non_api_ldy_non_appdown_site_unitid("unit_id"), "left_outer")
      .where("unit_id is null")
      .drop("ideaid", "siteid", "is_api_callback", "unit_id", "audit")
      .distinct()
      .withColumn("conversion_target", lit(Array[String]("none")))
    println("non_site: " + non_api_ldy_non_appdown_none_site_units.count())

    //2.2.3.2 通过非应用下载行业 所有建站unit_id取出所有建站数据
    non_api_ldy_non_appdown.join(non_api_ldy_non_appdown_site_unitid, non_api_ldy_non_appdown("unitid") === non_api_ldy_non_appdown_site_unitid("unit_id"))
      .join(sites, col("siteid") === col("site_id"), "left_outer")
      .select(
        col("userid"),
        col("adclass"),
        col("planid"),
        col("interaction"),
        col("unitid"),
        col("is_ocpc"),
        col("adslot_type"),
        col("ideas"),
        col("siteid"),
        col("conversion_goal"),
        col("target_medias"),
        expr("if(base_template is null, '', base_template)").alias("base_template")
      )
      .where("siteid>0") //建站中过滤非建站数据
      .distinct()
      .createOrReplaceTempView("unit_site_template")

    /*
     --加粉
     创业类：创业自营销
     彩票类：彩票模板
     医护类：减肥模板、保健品模板、功能化妆品、医护类自营销
     --表单
     招商加盟：招商加盟
     医美：医美模板
     电商：电商模板
     --赤兔表单
    */
    spark.udf.register("getConversionTargetByBaseTemplate", (isChituSite: Int, base_template: String) => {
      var conversion_target: String = ""
      val site_template_jiafen = Array("onlineTemplate", "lotteryTemplate", "fatTemplate", "healthTemplate", "huazhuangpinTemplate", "meirongTemplate")
      val site_template_form = Array("businessTemplate", "mediFatTemplate", "elecbusinessTemplate")

      if (isChituSite == 0 && site_template_jiafen.contains(base_template)) {
        conversion_target = "sdk_site_wz"
      }
      else if ((isChituSite == 0 && site_template_form.contains(base_template)) || isChituSite == 1) {
        conversion_target = "site_form"
      }
      else {
        conversion_target = "site_uncertain"
      }

      conversion_target
    })

    //2.2.3.2 计算非应用下载行业 所有建站数据 转化目标
    val non_api_ldy_non_appdown_site_units = spark.sql(
      s"""
         |select
         |  userid
         |  ,adclass
         |  ,planid
         |  ,interaction
         |  ,unitid
         |  ,is_ocpc
         |  ,adslot_type
         |  ,ideas
         |  ,if(siteid<5000000,0,1) as isChituSite
         |  ,conversion_goal
         |  ,target_medias
         |  ,getConversionTargetByBaseTemplate(if(siteid<5000000,0,1), base_template) as conversion_target
         |from unit_site_template
       """.stripMargin
    ).drop("isChituSite").distinct()

    /*
    * 过滤套户（即存在多个转化目标）
    * 有如下情况：
    *   sdk_site_wz  site_uncertain  --这种情况保留sdk_site_wz
    *   site_form    site_uncertain  --这种情况保留site_form
    *   sdk_site_wz  site_form  --套户
    *   sdk_site_wz  site_form  site_uncertain  --套户
    */

    val non_api_ldy_non_appdown_site_units_filter = non_api_ldy_non_appdown_site_units.groupBy(
      col("userid"),
      col("adclass"),
      col("planid"),
      col("interaction"),
      col("unitid"),
      col("is_ocpc"),
      col("adslot_type"),
      col("ideas"),
      col("conversion_goal"),
      col("target_medias")
    ).agg(
      expr("collect_set(conversion_target)").alias("conversion_target")
    ).select(//conversion_target数组删除site_uncertain值
      col("userid"),
      col("adclass"),
      col("planid"),
      col("interaction"),
      col("unitid"),
      col("is_ocpc"),
      col("adslot_type"),
      col("ideas"),
      col("conversion_goal"),
      col("target_medias"),
      expr("if(size(conversion_target)=2 and array_contains(conversion_target,'site_uncertain'), if(conversion_target[0]='site_uncertain', array(conversion_target[1]), array(conversion_target[0])), if(size(conversion_target)>=2 ,array('site_uncertain'), conversion_target))").alias("conversion_target")
    )
    println("site: " + non_api_ldy_non_appdown_site_units_filter.count())

//    api_units.printSchema()
//    println("#####")
//    non_api_appdown_units.printSchema()
//    println("#####")
//    non_api_ldy_appdown_units.printSchema()
//    println("#####")
//    non_api_ldy_dsgw_units.printSchema()
//    println("#####")
//    non_api_ldy_non_appdown_none_site_units.printSchema()
//    println("#####")
//    non_api_ldy_non_appdown_site_units_filter.printSchema()

    val result = api_units
      .union(non_api_appdown_units)
      .union(non_api_ldy_appdown_units)
      .union(non_api_ldy_dsgw_units)
      .union(non_api_ldy_non_appdown_none_site_units)
      .union(non_api_ldy_non_appdown_site_units_filter)
      .withColumn("day", lit(date))
      .createOrReplaceTempView("result_cvr_target")

//    insert overwrite table $dbName.dw_unitid_detail

    val sqlRequest1 =
      s"""
         |select
         |   userid
         |  ,planid
         |  ,unitid
         |  ,adclass
         |  ,interaction
         |  ,is_ocpc
         |  ,is_ocpc as src
         |  ,adslot_type
         |  ,case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds" --二类电商
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc" --应用商城
         |        when adclass in (110110100, 125100100) then "wzcp" --网赚彩票
         |        else "others"
         |   end as industry
         |  ,cast(split(ideas,',') as array<int>) as ideaids
         |  ,conversion_target as original_conversion_target
         |  ,conversion_goal
         |  ,target_medias
         |  ,case
         |      when is_ocpc=1 and conversion_goal=1 then split('sdk_app_install', ',')
         |      when is_ocpc=1 and conversion_goal=2 then split('api', ',')
         |      when is_ocpc=1 and conversion_goal=3 then split('site_form', ',')
         |      when is_ocpc=1 and conversion_goal=4 then split('sdk_site_wz', ',')
         |      else conversion_target
         |   end as conversion_target
         |  ,day
         |from result_cvr_target
       """.stripMargin
    println(sqlRequest1)
    val rawResult = spark.sql(sqlRequest1).cache()

//    rawResult
//        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_data20190628")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  split(conversion_target, ',') as history_conversion
         |FROM
         |  dl_cpc.unit_conversion_target_daily_version
         |WHERE
         |  `date` = '$date'
       """.stripMargin
    println(sqlRequest2)
    val unitConversion = spark.sql(sqlRequest2)

    val resultDF = rawResult
        .join(unitConversion, Seq("unitid"), "left_outer")
        .withColumn("conversion_target", when(col("industry") === "others" && col("history_conversion").isNotNull && col("is_ocpc") === 0, col("history_conversion")).otherwise(col("conversion_target")))
        .cache()

    resultDF.show(10)



    val tableName = s"$dbName.dw_unitid_conversion_target_version"
    resultDF.show(10)
    resultDF
        .select(
          "userid",
          "planid",
          "unitid",
          "adclass",
          "interaction",
          "is_ocpc",
          "src",
          "adslot_type",
          "industry",
          "ideaids",
          "conversion_target",
          "day"
        )
//        .repartition(10).write.mode("overwrite").insertInto(tableName)
        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_data20190910")

    println("-- write dw_unitid_detail into hdfs successfully --")

    union.unpersist()
    spark.close()
  }
}
