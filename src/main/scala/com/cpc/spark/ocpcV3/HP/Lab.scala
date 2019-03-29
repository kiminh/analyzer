package com.cpc.spark.ocpcV3.HP

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.sql.{Connection, DriverManager}
import com.typesafe.config.ConfigFactory

object Lab {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("directOcpcMonitor").enableHiveSupport().getOrCreate()
    val date = args(0).toString

    val indirect_ocpc_unit = getIndirectOcpcUnit(spark, date) // identifier, if_ocpc
    indirect_ocpc_unit.write.mode("overwrite").saveAsTable("test.indirect_ocpc_unit_sjq")
    val indirect_unit = getIndirectUnit(spark, date, indirect_ocpc_unit) // identifier, cost_hottopic, cost_qtt, if_ocpc
    indirect_unit.write.mode("overwrite").saveAsTable("test.indirect_unit_sjq")
//    val result = getContrastData(spark, date, indirect_unit)





    //    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    //    val jdate1 = sdf.parse(date)
    //    val calendar = Calendar.getInstance
    //    calendar.setTime(jdate1)
    //    calendar.add( Calendar.DATE, -1 )
    //    val jdate0 = calendar.getTime
    //    val date0 = sdf.format(jdate0)

    //    create table if not exists test.directOcpcMonitor
    //    (
    //      label      STRING COMMENT '直暗投，ocpc/cpc',
    //    show_n     INT    COMMENT '展示量',
    //    ctr        FLOAT  COMMENT '点击率',
    //    click_n    INT    COMMENT '点击数',
    //    click_cvr  FLOAT  COMMENT '转化率',
    //    cvr_n      INT    COMMENT '转化数',
    //    money      FLOAT  COMMENT '收入',
    //    cpa        FLOAT  COMMENT 'cost per action',
    //    cpa_given  FLOAT  COMMENT 'cpa_given',
    //    cpm        FLOAT  COMMENT 'cost per mille',
    //    total_arpu FLOAT  COMMENT 'arpu',
    //    acp        FLOAT  COMMENT 'average click price'
    //    )
    //    COMMENT '热点段子明暗投、ocpc/cpc监控日报'
    //    PARTITIONED BY (`date` STRING);
    //
    //    val sqlRequest =
    //      s"""
    //         |  select
    //         |   case
    //         |     when c.identifier is not NULL and exptags like "%hot_topic%" and length(ext_string['ocpc_log']) > 0  then '暗投ocpc'
    //         |     when c.identifier is not NULL and                                length(ext_string['ocpc_log']) = 0  then '暗投cpc'
    //         |     else '直投'
    //         |    end as label, --这里的ocpc是指付费方式为cpc下面的ocpc
    //         |   sum(isshow)                                                                    as show_n,
    //         |   round(sum(isclick)*100                                   /sum(isshow),      3) as ctr,       --单位：%
    //         |         sum(isclick)                                                             as click_n,
    //         |   round(sum(iscvr)*100                                     /sum(isclick),     3) as click_cvr, --单位：%
    //         |         sum(iscvr)                                                               as cvr_n,
    //         |   round(sum(case WHEN isclick = 1 then price else 0 end)/100, 3)                 as money,
    //         |   round(sum(case WHEN isclick = 1 then price else 0 end)   /sum(100*iscvr),   3) as cpa,       --单位：元
    //         |   round(sum(isclick*float(substring(ocpc_log, locate("cpagiven:", ocpc_log) + 9, locate("pcvr", ocpc_log) - locate(",cpagiven:", ocpc_log) - 11)))/sum(isclick) ) as cpa_given,
    //         |   round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow),      3) as cpm,       --cpc下面的cpm，单位：元
    //         |   (sum(case WHEN isclick = 1 and (ext["charge_type"].int_value = 1 or ext["charge_type"] IS NULL ) then price else 0 end)/100
    //         |  + sum(case when isshow  = 1 and  ext["charge_type"].int_value = 2                                 then price else 0 end)/100000.0 )
    //         |   /count(distinct uid)                                                           as total_arpu,
    //         |   round(sum(case WHEN isclick = 1 then price else 0 end)/100, 3)/sum(isclick)    as acp,
    //         |   '$date'                                                                        as `date`
    //         |FROM
    //         |     (  select
    //         |         ext_string['ocpc_log'] as ocpc_log,
    //         |         cast(unitid as string) as identifier,
    //         |         *
    //         |        from dl_cpc.cpc_hot_topic_union_log
    //         |       WHERE `date` = '$date'
    //         |         and isshow = 1 --已经展示
    //         |         and media_appsid = '80002819'
    //         |         and ext['antispam'].int_value = 0  --反作弊标记：1作弊，0未作弊
    //         |         AND userid > 0 -- 广告主id
    //         |         and adsrc = 1  -- cpc广告（我们这边的广告，非外部广告）
    //         |         AND ( ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1 ) --charge_type: 计费类型
    //         |     ) a
    //         |left join
    //         |     (
    //         |       select
    //         |        searchid,
    //         |        label2 as iscvr --是否转化
    //         |       from dl_cpc.ml_cvr_feature_v1
    //         |      WHERE `date` = '$date'
    //         |     ) b on a.searchid = b.searchid
    //         |left join
    //         |     (
    //         |        select
    //         |          identifier
    //         |        from
    //         |          dl_cpc.ocpc_pb_result_hourly  --该表中的identifier已经去直投了
    //         |        where
    //         |          ((`date` = '$date0' and hour >= '21') or (`date` = '$date' and hour < '21'))
    //         |          and version = 'hottopicv1'
    //         |        group by
    //         |          identifier
    //         |      ) c on a.identifier = c.identifier
    //         |GROUP BY
    //         |   case
    //         |     when c.identifier is not NULL and exptags like "%hot_topic%" and length(ext_string['ocpc_log']) > 0  then '暗投ocpc'
    //         |     when c.identifier is not NULL and                                length(ext_string['ocpc_log']) = 0  then '暗投cpc'
    //         |     else '直投'
    //         |    end
    //       """.stripMargin
    //
    //    val df = spark.sql(sqlRequest)
    //    df.write.mode("overwrite").insertInto("test.directOcpcMonitor")
    //    val df2 = df.select("label", "show_n", "ctr", "click_n", "click_cvr", "cvr_n", "money", "cpa", "cpm", "total_arpu", "acp", "`date`")

    //    val reportTable = "report2.direct_ocpc_monitor"
    //    val deleteSQL = s"delete from $reportTable where date = '$date'"
    //    update(deleteSQL)
    //    insert(df2, reportTable)

  }

  def getIndirectOcpcUnit(spark: SparkSession, date: String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val jdate1 = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(jdate1)
    calendar.add(Calendar.DATE, -1)
    val jdate0 = calendar.getTime
    val date0 = sdf.format(jdate0)
    val sql =
      s"""
         |select
         |  identifier,
         |  1 as if_ocpc
         |from
         |  dl_cpc.ocpc_pb_result_hourly  --该表中的identifier已经去直投了
         |where
         |  ((`date` = '$date0' and hour >= '21') or (`date` = '$date' and hour < '21'))
         |  and version = 'hottopicv1'
       """.stripMargin
    val df = spark.sql(sql).distinct().toDF()
    df
  }

  def getIndirectUnit(spark: SparkSession, date: String, indrectOcpc: DataFrame) = {
    val sql =
      s"""
         |select
         |  cast( unitid as string ) as identifier,
         |  sum( case when media_appsid =  '80002819' then price else 0 end ) as cost_hottopic,
         |  sum( case when media_appsid <> '80002819' then price else 0 end ) as cost_qtt
         |from dl_cpc.slim_union_log a
         |where dt = '$date'
         |  and adsrc = 1
         |  and isshow = 1
         |  and isclick = 1
         |  and media_appsid in ("80000001", "80000002", "80002819")
         |  and (charge_type is NULL or charge_type = 1)
         |  and userid > 0
         |  and antispam = 0
         |group by unitid
       """.stripMargin

    val indirectUnit = spark.sql(sql)
      .filter("cost_hottopic > 0 and cost_qtt > 0")

    val indirectCpcUnit = indirectUnit
      .join(indrectOcpc, Seq("identifier"), "left")
      .select("identifier", "cost_hottopic", "cost_qtt", "if_ocpc")
      .na.fill(0, Seq("if_ocpc"))
    indirectCpcUnit
  }

//  def getContrastData(spark: SparkSession, date: String, indirectUnit: DataFrame): Unit = {
//    indirectUnit.createOrReplaceTempView("indirectUnit")
//    val sql =
//      s"""
//         |
//       """.stripMargin
//  }


  //  def update(sql: String): Unit ={
  //    val conf = ConfigFactory.load()
  //    val url      = conf.getString("mariadb.report2_write.url")
  //    val driver   = conf.getString("mariadb.report2_write.driver")
  //    val username = conf.getString("mariadb.report2_write.user")
  //    val password = conf.getString("mariadb.report2_write.password")
  //    var connection: Connection = null
  //    try{
  //      Class.forName(driver) //动态加载驱动器
  //      connection = DriverManager.getConnection(url, username, password)
  //      val statement = connection.createStatement
  //      val rs = statement.executeUpdate(sql)
  //      println(s"EXECUTE $sql SUCCESS!")
  //    }
  //    catch{
  //      case e: Exception => e.printStackTrace
  //    }
  //    connection.close  //关闭连接，释放资源
  //  }
  //
  //  def insert(data:DataFrame, table: String): Unit ={
  //    val conf = ConfigFactory.load()
  //    val mariadb_write_prop = new Properties()
  //
  //    val url      = conf.getString("mariadb.report2_write.url")
  //    val driver   = conf.getString("mariadb.report2_write.driver")
  //    val username = conf.getString("mariadb.report2_write.user")
  //    val password = conf.getString("mariadb.report2_write.password")
  //
  //    mariadb_write_prop.put("user", username)
  //    mariadb_write_prop.put("password", password)
  //    mariadb_write_prop.put("driver", driver)
  //
  //    data.write.mode(SaveMode.Append)
  //      .jdbc(url, table, mariadb_write_prop)
  //    println(s"INSERT INTO $table SUCCESSFULLY!")
  //
  //  }

}








