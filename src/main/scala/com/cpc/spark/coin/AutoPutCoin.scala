package com.cpc.spark.coin
import java.io.FileOutputStream

import com.redis.RedisClient
import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import autoCoin.autoCoin._
/**
  * @author Jinbao
  * @date 2018/11/8 14:08
  */
object AutoPutCoin {
    def main(args: Array[String]): Unit = {

        val cpc_api_union_log = args(0) //"dl_cpc.cpc_api_union_log"

        val ml_cvr_feature_v1 = args(1) //"test.ml_cvr_feature_v1"

        val coinTable = "test.coin3" //TODO

        val date = args(2)

        val hour = args(3).toInt

        val minute = 0

        val p = args(4).toDouble //0.7

        val preDay = args(5).toInt //3

        val spark = SparkSession.builder()
          .appName(s"AutoPutCoin date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val datehourlist = scala.collection.mutable.ListBuffer[String]()
        val cal = Calendar.getInstance()
        cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour, minute)
        for (t <- 0 to 24 * preDay) {
            if (t > 0) {
                cal.add(Calendar.HOUR, -1)
            }
            val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
            val dd = sf.format(cal.getTime())
            val d1 = dd.substring(0, 10)
            val h1 = dd.substring(11, 13)
            val datecond = s"`date` = '$d1' and hour = '$h1'"
            if (!(d1 == "2019-02-18" || d1 == "2019-02-17" || d1== "2019-02-16"))
                datehourlist += datecond
        }

        val datehour = datehourlist.toList.mkString(" or ")

        val metricsSql =
            s"""
               |select userid
               |from dl_cpc.cpc_report_coin_userid_metrics
               |where `date`=date_sub('$date',1)
               |and auc <= 0.5
               |and coin_click_num >= 50
             """.stripMargin

        val useridBlacklist = spark.sql(metricsSql).rdd.map(x => x.getAs[Int]("userid").toString).collect().toList.mkString(",")

        println(useridBlacklist)

        val ideaBlacklist =
            """
              |2391911, 2385002, 2388289, 2391495, 2381868, 2391641, 2330249, 2384970, 2391533, 2360176, 2391895, 2391881, 2390834
            """.stripMargin

        val userWhiteList =
            """
              |1522853,1534763,1533743,1538013,1538252,1515505,1552588,1549938,1546988,1557909,1552587, 1553711, 1552154, 1559014, 1551248, 1538486, 1551950, 1559495, 1550108, 1551707, 1548568, 1558766, 1558705, 1559522, 1556256, 1548521, 1558838, 1559266, 1560432, 1559076, 1557344, 1540595, 1560225
            """.stripMargin

        val apiUnionLogSql =
            s"""
               |select ideaid,ext["exp_cvr"].int_value as exp_cvr
               |from $cpc_api_union_log
               |where ($datehour)
               |and iscvr = 1
               |and media_appsid in ('80000001','80000002')
               |and ideaid > 0
               |and adslot_type in (1, 2)
               |and round(ext['adclass'].int_value/1000000) != 107 and round(ext['adclass'].int_value/1000000) != 134
               |and ((adslot_type<>7 and ext['adclass'].int_value like '100%') or (ext['adclass'].int_value in (110110100, 125100100)))
               |and ideaid not in ($ideaBlacklist)
               |and userid not in ($useridBlacklist)
               |and (userid in ($userWhiteList) or ext['usertype'].int_value != 2)
             """.stripMargin
        println(apiUnionLogSql)
        val apiUnionLog = spark.sql(apiUnionLogSql)
        println("apiUnionLog 's count is " + apiUnionLog.rdd.count())
        val apiUnionNth = getNth(apiUnionLog, p)

        println("apiUnionNth 's count is " + apiUnionNth.count())
        val mlFeatureSql =
            s"""
               |select a.ideaid as ideaid,exp_cvr
               |    from
               |    (
               |        select ideaid,exp_cvr
               |        from dl_cpc.ml_cvr_feature_v1
               |        where ($datehour)
               |        and label2 = 1
               |        and media_appsid in ('80000001','80000002')
               |        and adslot_type in (1, 2)
               |        and ideaid > 0
               |        and round(adclass/1000000) != 107 and round(adclass/1000000) != 134
               |        and ((adslot_type<>7 and adclass like '100%') or (adclass in (110110100, 125100100)))
               |    ) a left outer join
               |    (
               |        select x.id as ideaid ,x.user_id as userid,y.account_type as account_type
               |        from
               |        (
               |            select id,user_id
               |            from src_cpc.cpc_idea
               |            group by id,user_id
               |         ) x left outer join
               |         (
               |            select id, account_type
               |            from src_cpc.cpc_user_p
               |            where account_type = 2
               |            group by id, account_type
               |         ) y
               |         on x.user_id = y.id
               |    ) b
               |    on a.ideaid = b.ideaid
               |    where
               |    a.ideaid not in ($ideaBlacklist)
               |    and b.userid not in ($useridBlacklist)
               |    and (b.userid in ($userWhiteList) or b.account_type is null)
             """.stripMargin
        println(mlFeatureSql)
        val mlFeature = spark.sql(mlFeatureSql)
        println("mlFeature 's count is " + mlFeature.rdd.count())

        val mlFeatureNth = getNth(mlFeature, p)

        println("mlFeatureNth 's count is " + mlFeatureNth.count())

        val Nth = mlFeatureNth.fullOuterJoin(apiUnionNth)
          .map(x => {
              val label = x._2._1.orNull

              val api = x._2._2.orNull
              if (label != null && api != null) {
                  coin(ideaid = x._1,
                      label_exp_cvr = label._1,
                      label_min = label._2,
                      label_max = label._3,
                      label_num = label._4,
                      label_5th = label._5,
                      label_6th = label._6,
                      label_7th = label._7,
                      label_8th = label._8,
                      label_9th = label._9,

                      api_exp_cvr = api._1,
                      api_min = api._2,
                      api_max = api._3,
                      api_num = api._4,
                      api_5th = api._5,
                      api_6th = api._6,
                      api_7th = api._7,
                      api_8th = api._8,
                      api_9th = api._9,

                      date = date,
                      hour = hour.toString)
              }
              else if (label == null && api != null) {
                  coin(ideaid = x._1,

                      api_exp_cvr = api._1,
                      api_min = api._2,
                      api_max = api._3,
                      api_num = api._4,
                      api_5th = api._5,
                      api_6th = api._6,
                      api_7th = api._7,
                      api_8th = api._8,
                      api_9th = api._9,

                      date = date,
                      hour = hour.toString)
              }
              else if (label != null && api == null) {
                  coin(ideaid = x._1,
                      label_exp_cvr = label._1,
                      label_min = label._2,
                      label_max = label._3,
                      label_num = label._4,
                      label_5th = label._5,
                      label_6th = label._6,
                      label_7th = label._7,
                      label_8th = label._8,
                      label_9th = label._9,

                      date = date,
                      hour = hour.toString)
              }
              else {
                  coin(ideaid = x._1,
                      date = date,
                      hour = hour.toString)
              }
          })
          .filter(x => x.api_num >= 30 || x.label_num >= 30)
          .toDS()
          .coalesce(1)

        println("Nth 's count is " + Nth.count())

        Nth.write.mode("overwrite").insertInto(coinTable)



        //保存到PB文件

        val autoCoinListBuffer = scala.collection.mutable.ListBuffer[AutoCoin]()

        Nth.collect().foreach(coin => {
            autoCoinListBuffer += AutoCoin(
                ideaid = coin.ideaid,
                label2ExpCvr = coin.label_exp_cvr,
                apiExpCvr = coin.api_exp_cvr)
        })

        val autoCoinList = autoCoinListBuffer.toArray
        println("autoCoinList 's num is " + autoCoinList.length)
        val coinData = Coin(coin = autoCoinList)

        coinData.writeTo(new FileOutputStream("Coin.pb"))

        println("write to Coin.pb success")

        spark.stop()
    }

    def getNth(df: DataFrame, p: Double): RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        df.rdd.map(x => (x.getAs[Int]("ideaid"), x.getAs[Int]("exp_cvr")))
          .combineByKey(x => List(x),
              (x: List[Int], y: Int) => y :: x,
              (x: List[Int], y: List[Int]) => x ::: y)
          .mapValues(x => {
              val sorted = x.sorted
              val index = (sorted.length * p).toInt
              val i5th = (sorted.length * 0.5).toInt
              val i6th = (sorted.length * 0.6).toInt
              val i7th = (sorted.length * 0.7).toInt
              val i8th = (sorted.length * 0.8).toInt
              val i9th = (sorted.length * 0.9).toInt
              (sorted(index), sorted(0), sorted(sorted.length - 1), sorted.length,
                sorted(i5th), sorted(i6th), sorted(i7th), sorted(i8th), sorted(i9th))
              //x(index)
          })
    }
    def getThreshold(spark:SparkSession,df:DataFrame,date:String,default_p:Double): RDD[(Int, (Int, Int, Int, Int, Int, Int, Int, Int, Int))] = {
        val sql =
            s"""
               |select ideaid,p
               |from dl_cpc.cpc_auto_coin_idea_threshold
               |where `date`='$date'
             """.stripMargin

        val ideaP = spark.sql(sql)

        val r = df.join(ideaP,Seq("ideaid"),"left_outer").select("ideaid","exp_cvr","p")
          .na
          .fill(default_p,Seq("p"))
          .rdd
          .map(x => (x.getAs[Int]("ideaid"),
            (List(x.getAs[Int]("exp_cvr")),x.getAs[Double]("p"))))
          .reduceByKey((x,y) => {
              val t1 = x._1 ::: y._1
              val t2 = if (x._2 > y._2) x._2 else y._2
              (t1,t2)
          })
          .mapValues(x => {
              val cvrlist = x._1
              val p = x._2
              val sorted = cvrlist.sorted
              val index = (sorted.length * p).toInt
              val i5th = (sorted.length * 0.5).toInt
              val i6th = (sorted.length * 0.6).toInt
              val i7th = (sorted.length * 0.7).toInt
              val i8th = (sorted.length * 0.8).toInt
              val i9th = (sorted.length * 0.9).toInt
              (sorted(index), sorted(0), sorted(sorted.length - 1), sorted.length,
                sorted(i5th), sorted(i6th), sorted(i7th), sorted(i8th), sorted(i9th))
          })
        r
    }

    case class coin(var ideaid: Int = 0,
                    var label_exp_cvr: Int = 9999999,
                    var label_min: Int = 9999999,
                    var label_max: Int = 9999999,
                    var label_num: Int = 0,
                    var label_5th: Int = 9999999,
                    var label_6th: Int = 9999999,
                    var label_7th: Int = 9999999,
                    var label_8th: Int = 9999999,
                    var label_9th: Int = 9999999,

                    var api_exp_cvr: Int = 9999999,
                    var api_min: Int = 9999999,
                    var api_max: Int = 9999999,
                    var api_num: Int = 0,
                    var api_5th: Int = 9999999,
                    var api_6th: Int = 9999999,
                    var api_7th: Int = 9999999,
                    var api_8th: Int = 9999999,
                    var api_9th: Int = 9999999,
                    var date: String = "",
                    var hour: String = "")
}

