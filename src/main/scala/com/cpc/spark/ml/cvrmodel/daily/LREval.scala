package com.cpc.spark.ml.cvrmodel.daily

import com.cpc.spark.ml.dnn.Utils.DateUtils
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * @author fym
  * @version created: 2019-05-28 10:53
  * @desc
  */
object LREval {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("LREval").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val date = args(0)
    val tomorrow=DateUtils.getPrevDate(date, -1)

//    val beforeSql=
//      """
//        |create temporary function auc as 'hivemall.evaluation.AUCUDAF' using jar "/home/cpc/anal/lib/hivemall-all-0.5.2-incubating.jar"
//      """.stripMargin
//    spark.sql(beforeSql)

    val bs_sql =
      s"""
         |insert overwrite table dl_cpc.pass_back_label_test_qizhi
         |select searchid
         | , uid
         | , day
         | , hour
         | , sex
         | , age
         | , os
         | , isp
         | , network
         | , city
         | , media_appsid
         | , adslotid
         | , phone_level
         | , adclass
         | , adtype
         | , planid
         | , unitid
         | , ideaid,IF(rank=1,1,0) as label from
         |(select searchid
         | , uid
         | , day
         | , hour
         | , sex
         | , age
         | , os
         | , isp
         | , network
         | , city
         | , media_appsid
         | , adslotid
         | , phone_level
         | , adclass
         | , adtype
         | , planid
         | , unitid
         | , ideaid
         | , Row_Number() OVER (partition by uid ORDER BY `timestamp` desc) as rank from
         |(select searchid
         | , uid
         | , day
         | , hour
         | , sex
         | , age
         | , os
         | , isp
         | , network
         | , city
         | , media_appsid
         | , adslotid
         | , phone_level
         | , adclass
         | , adtype
         | , planid
         | , unitid
         | , ideaid
         | , `timestamp` from
         |(select imei,dt,date_add(dt,-7) as before7dt from dl_cpc.pass_back_test_qizhi)t1
         |join
         |(select
         |   searchid
         | , md5(uid) as uid
         | , day
         | , hour
         | , sex
         | , age
         | , os
         | , isp
         | , network
         | , city
         | , media_appsid
         | , adslotid
         | , phone_level
         | , adclass
         | , adtype
         | , planid
         | , unitid
         | , ideaid
         | , `timestamp`
         |from dl_cpc.cpc_basedata_click_event
         |where day>='2019-08-01' and day<='2019-09-09'
         |  and media_appsid in ('80000001','80000002')
         |  and adslot_type in (1, 2)
         |  and isclick = 1
         |  and ideaid > 0
         |  and unitid > 0
         |  and userid in (1562482,1568203,1582093,1598145,1604411,1616036,1616821,1629979,1629982,1638653,1641461,1641463,1641469,1588335,1594022,1641470,1653235,1656197,1665043,1673378,1666928,1629538,1629537,1569962) )t2
         |on t1.imei=t2.uid and dt<=day and before7dt>=day)t3)t4
      """.stripMargin
    println("sql="+bs_sql)
    val bs_df=spark.sql(bs_sql)
    println("============= bs_df ===============")
    bs_df.show(5)

  }
}