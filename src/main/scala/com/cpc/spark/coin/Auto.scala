package com.cpc.spark.coin

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/3/5 11:15
  */
object Auto {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"Auto date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0)
        cal.add(Calendar.HOUR, -1)

        val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dd = sf.format(cal.getTime())
        val d1 = dd.substring(0, 10)
        val h1 = dd.substring(11, 13)

        println(date,hour,d1,h1)

        val sql =
            s"""
               |select
               |    a.usertype as usertype,
               |    a.userid as userid,
               |    a.ideaid as ideaid,
               |    sum(if(label_5th<=exp_cvr,1,0)) as label_5th_show_num,
               |    sum(if(label_6th<=exp_cvr,1,0)) as label_6th_show_num,
               |    sum(if(label_7th<=exp_cvr,1,0)) as label_7th_show_num,
               |    sum(if(label_8th<=exp_cvr,1,0)) as label_8th_show_num,
               |    sum(if(label_9th<=exp_cvr,1,0)) as label_9th_show_num,
               |
               |    sum(if(label_5th<=exp_cvr and isclick=1,1,0)) as label_5th_click_num,
               |    sum(if(label_6th<=exp_cvr and isclick=1,1,0)) as label_6th_click_num,
               |    sum(if(label_7th<=exp_cvr and isclick=1,1,0)) as label_7th_click_num,
               |    sum(if(label_8th<=exp_cvr and isclick=1,1,0)) as label_8th_click_num,
               |    sum(if(label_9th<=exp_cvr and isclick=1,1,0)) as label_9th_click_num,
               |
               |    sum(if(api_5th<=exp_cvr,1,0)) as api_5th_show_num,
               |    sum(if(api_6th<=exp_cvr,1,0)) as api_6th_show_num,
               |    sum(if(api_7th<=exp_cvr,1,0)) as api_7th_show_num,
               |    sum(if(api_8th<=exp_cvr,1,0)) as api_8th_show_num,
               |    sum(if(api_9th<=exp_cvr,1,0)) as api_9th_show_num,
               |
               |    sum(if(api_5th<=exp_cvr and isclick=1,1,0)) as api_5th_click_num,
               |    sum(if(api_6th<=exp_cvr and isclick=1,1,0)) as api_6th_click_num,
               |    sum(if(api_7th<=exp_cvr and isclick=1,1,0)) as api_7th_click_num,
               |    sum(if(api_8th<=exp_cvr and isclick=1,1,0)) as api_8th_click_num,
               |    sum(if(api_9th<=exp_cvr and isclick=1,1,0)) as api_9th_click_num,
               |
               |    '$date' as date,
               |    '$hour' as hour
               |
               |from
               |(
               |    select ext['usertype'].int_value as usertype,
               |        userid,
               |        ideaid,
               |        isclick,
               |        ext['exp_cvr'].int_value as exp_cvr
               |    from dl_cpc.cpc_union_log
               |        where `date`='$date' and hour = '$hour'
               |        and media_appsid  in ("80000001", "80000002")
               |        and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
               |        and round(ext["adclass"].int_value/1000) != 132101
               |        and adslot_type in (1,2)
               |) a join
               |(
               |    select ideaid,
               |        label_5th, label_6th, label_7th, label_8th, label_9th,
               |        api_5th, api_6th, api_7th, api_8th, api_9th
               |    from test.coin3
               |    where `date`='$d1' and hour = '$h1'
               |) b
               |on a.ideaid = b.ideaid
               |group by a.usertype, a.userid, a.ideaid
             """.stripMargin

        println(sql)

        val data = spark.sql(sql)

        data.repartition(1).write.mode("overwrite").insertInto("test.coin_tmp_20190305")

        println("insert into done !")
    }
}
