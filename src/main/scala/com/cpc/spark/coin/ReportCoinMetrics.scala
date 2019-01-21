package com.cpc.spark.coin

import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.cpc.spark.tools.CalcMetrics
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Jinbao
  * @date 2018/12/17 20:33
  */
object ReportCoinMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"ReportCoinMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val unionSql =
            s"""
               |select ideaid,
               |    isshow,
               |    ext_int,
               |    isclick,
               |    label2,
               |    price,
               |    uid,
               |    userid,
               |    ext
               |from
               |    (
               |        select searchid,ideaid,isshow,ext_int,isclick,price,uid,userid,ext
               |        from dl_cpc.cpc_union_log
               |        where `date`='$date'
               |        and media_appsid  in ("80000001", "80000002") and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
               |        and round(ext["adclass"].int_value/1000) != 132101
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='$date'
               |    ) b
               |    on a.searchid = b.searchid
             """.stripMargin

        val union = spark.sql(unionSql)
        //保存到临时表里
        //union.write.mode("overwrite").saveAsTable("test.union")

        union.createOrReplaceTempView("union")

//        val ideaidSql =
//            s"""
//               |select
//               |    ideaid,
//               |    show_num,
//               |    coin_show_num,
//               |    if (show_num!=0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
//               |    click_num,
//               |    coin_click_num,
//               |    if (click_num!=0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
//               |    if (show_num!=0,round(click_num/show_num, 6),0) as ctr,
//               |    if (coin_show_num!=0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
//               |    convert_num,
//               |    coin_convert_num,
//               |    if (convert_num!=0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
//               |    if (click_num!=0,round(convert_num/click_num, 6),0) as cvr,
//               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
//               |    click_total_price,
//               |    coin_click_total_price,
//               |    uid_num,
//               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
//               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
//               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
//               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
//               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
//               |    '$date' as `date`
//               |from
//               |(
//               |    select ideaid,
//               |        sum(isshow) as show_num, --展示数
//               |        sum(if (isshow=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_show_num, --金币展示数
//               |        sum(isclick) as click_num, --点击数
//               |        sum(if (isclick=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_click_num, --金币点击数
//               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
//               |        sum(case when label2 = 1 and ext_int['is_auto_coin'] = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
//               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
//               |        sum(case WHEN isclick = 1 and ext_int['is_auto_coin'] = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
//               |        count(distinct uid) as uid_num --用户数
//               |    from union
//               |    group by ideaid
//               |) c
//             """.stripMargin
//
//        val ideaidMetrics = spark.sql(ideaidSql).cache()
//
//        ideaidMetrics.repartition(1)
//          .write
//          .mode("overwrite")
//          .insertInto("dl_cpc.cpc_report_coin_ideaid_metrics")
//
        val conf = ConfigFactory.load()
        val mariadb_write_prop = new Properties()

        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))
//
//        val ideaidMetricsDelSql = s"delete from report2.report_coin_ideaid_metrics where `date` = '$date'"
//        OperateMySQL.del(ideaidMetricsDelSql)
//        ideaidMetrics.write.mode(SaveMode.Append)
//          .jdbc(mariadb_write_url, "report2.report_coin_ideaid_metrics", mariadb_write_prop)
//        println("insert into report2.report_coin_ideaid_metrics success!")
//        ideaidMetrics.unpersist()

//        val useridSql =
//            s"""
//               |select
//               |    userid,
//               |    show_num,
//               |    coin_show_num,
//               |    if (show_num!=0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
//               |    click_num,
//               |    coin_click_num,
//               |    if (click_num!=0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
//               |    if (show_num!=0,round(click_num/show_num, 6),0) as ctr,
//               |    if (coin_show_num!=0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
//               |    convert_num,
//               |    coin_convert_num,
//               |    if (convert_num!=0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
//               |    if (click_num!=0,round(convert_num/click_num, 6),0) as cvr,
//               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
//               |    click_total_price,
//               |    coin_click_total_price,
//               |    uid_num,
//               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
//               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
//               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
//               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
//               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
//               |    '$date' as `date`
//               |from
//               |(
//               |    select userid,
//               |        sum(isshow) as show_num, --展示数
//               |        sum(if (isshow=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_show_num, --金币展示数
//               |        sum(isclick) as click_num, --点击数
//               |        sum(if (isclick=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_click_num, --金币点击数
//               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
//               |        sum(case when label2 = 1 and ext_int['is_auto_coin'] = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
//               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
//               |        sum(case WHEN isclick = 1 and ext_int['is_auto_coin'] = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
//               |        count(distinct uid) as uid_num --用户数
//               |    from test.union
//               |    group by userid
//               |) c
//             """.stripMargin

        val useridAucListSql =
            s"""
               |select cast(userid as string) as userid,ext['exp_cvr'].int_value as score,label2 as label
               |from union
             """.stripMargin

        val useridAucList = spark.sql(useridAucListSql)

        val uAuc = CalcMetrics.getGauc(spark,useridAucList,"userid")
          .select("name","auc")
          .withColumn("userid",string2Int(col("name")))
          .drop("name")
          .select("userid","auc")
        //uAuc.write.mode("overwrite").saveAsTable("test.uauc")
        uAuc.createOrReplaceTempView("uauc")

        val useridSql =
            s"""
               |select
               |    c.userid as userid,
               |    show_num,
               |    coin_show_num,
               |    if (show_num!=0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
               |    click_num,
               |    coin_click_num,
               |    if (click_num!=0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
               |    if (show_num!=0,round(click_num/show_num, 6),0) as ctr,
               |    if (coin_show_num!=0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
               |    convert_num,
               |    coin_convert_num,
               |    if (convert_num!=0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
               |    if (click_num!=0,round(convert_num/click_num, 6),0) as cvr,
               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
               |    click_total_price,
               |    coin_click_total_price,
               |    uid_num,
               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
               |    auc,
               |    '$date' as `date`
               |from
               |(
               |    select userid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_click_num, --金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and ext_int['is_auto_coin'] = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and ext_int['is_auto_coin'] = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from union
               |    group by userid
               |) c
               |left outer join uauc d
               |on c.userid = d.userid
             """.stripMargin

        val useridMetrics = spark.sql(useridSql).cache()
//        useridOtherMetrics.show(10)
//        println("useridOtherMetrics 's count is " + useridOtherMetrics.count())
//        val useridMetrics = useridOtherMetrics
//          .join(uAuc,Seq("userid"),"left_outer")
//          .select("userid","show_num","coin_show_num","coin_show_rate","click_num","coin_click_num","coin_click_rate"
//          ,"ctr","coin_ctr","convert_num","coin_convert_num","coin_convert_rate","cvr","coin_cvr","click_total_price",
//          "coin_click_total_price","uid_num","cpm","acp","arpu","aspu","acpu","auc","date")
//          .cache()

        useridMetrics.show(20)

        useridMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_userid_metrics")

        val useridMetricsDelSql = s"delete from report2.report_coin_userid_metrics where `date` = '$date'"
        OperateMySQL.del(useridMetricsDelSql)
        useridMetrics.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, "report2.report_coin_userid_metrics", mariadb_write_prop)
        println("insert into report2.report_coin_userid_metrics success!")
        useridMetrics.unpersist()


    }
    def string2Int = udf((name:String) => {
        name.toInt
    })
}

/**
  * ALTER TABLE dl_cpc.cpc_report_coin_userid_metrics ADD COLUMNS (auc double);
  */
