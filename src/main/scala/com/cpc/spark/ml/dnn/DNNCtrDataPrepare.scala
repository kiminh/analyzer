package com.cpc.spark.ml.dnn

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * 获取dnn ctr训练的数据，生成tfrecord格式保存到 /user/dnn/ctr/traindata下
  * created time : 2018/9/25 15:15
  *
  * @author zhj
  * @version 1.0
  *
  */
object DNNCtrDataPrepare {
  def main(args: Array[String]): Unit = {

    val date = args(0) //"2018-09-25"

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //获取app数据
    val app_data = spark.sql(
      s"""
         |select uid,pkgs
         |from dl_cpc.cpc_user_installed_apps
         |where load_date='$date'
      """.stripMargin)

    /*val writer = new PrintWriter(new File("/home/cpc/zhj/app_map/map.txt"))
    val app_map = apps.select(explode($"pkgs").alias("pkg")).groupBy("pkg").count()
      .sort($"count".desc)
      .take(5000).zipWithIndex
      .map {
        x =>
          writer.write(x._1.getAs[String]("pkg") + "," + x._2 + "\n")
          (x._1.getAs[String]("pkg"), x._2)
      }.toMap
    writer.close()


    val a_p = spark.sparkContext.broadcast(app_map)

    val getAppVec = udf {
      pkgs: Seq[String] =>
        val m = a_p.value
        pkgs.map(pkg => m.getOrElse(pkg, -1)).filter(_ >= 0)
    }*/
    //获取最近3天广告点击数据
    val click_data1 = spark.sql(
      s"""
         |select uid,collect_set(ideaid) as ideaids
         |from dl_cpc.ml_cvr_feature_v1
         |where date > '${getDay(date, 3)}'
         |  and date <= '$date'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and label > 0
         |  and media_appsid in ('80000001','80000002')
         |group by uid
      """.stripMargin)

    //获取最近4到7天点击数据
    val click_data2 = spark.sql(
      s"""
         |select uid,collect_set(ideaid) as ideaids
         |from dl_cpc.ml_cvr_feature_v1
         |where date > '${getDay(date, 7)}'
         |  and date <= '${getDay(date, 4)}'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and label > 0
         |  and media_appsid in ('80000001','80000002')
         |group by uid
      """.stripMargin)

    //合并数据
    val data = spark.sql(
      s"""
         |select uid,hour,sex,age,os,network,city,
         |      adslotid,phone_level,adclass,
         |      adtype,planid,unitid,ideaid,
         |      if(label>0, array(1,0), array(0,1)) as label
         |from dl_cpc.ml_cvr_feature_v1
         |where date = '$date'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
      """.stripMargin)
      .join(app_data, Seq("uid"), "left")
      .join(click_data1, Seq("uid"), "left")
      .select($"label", hash("uid")($"uid").alias("uid"),
        hash("hour")($"hour").alias("hour"), hash("sex")($"sex").alias("sex"),

        hash("os")($"os").alias("os"), hash("network")($"network").alias("network"),

        hash("city")($"city").alias("city"), hash("adslotid")($"adslotid").alias("adslotid"),

        hash("phone_level")($"phone_level").alias("pl"), hash("adclass")($"adclass").alias("adclass"),

        hash("adtype")($"adtype").alias("adtype"), hash("planid")($"planid").alias("planid"),

        hash("unitid")($"unitid").alias("unitid"), hash("ideaid")($"ideaid").alias("ideaid"),

        hashSeq("app", "string")($"pkgs").alias("apps"), hashSeq("ideaids", "int")($"ideaids").alias("ideaids"))

      .select(array($"uid", $"hour", $"sex", $"os", $"network", $"city", $"adslotid", $"pl",
        $"adclass", $"adtype", $"planid", $"unitid", $"ideaid").alias("feature"),
        $"apps", $"ideaids", $"label")
      .persist()

    val Array(traindata, testdata) = data.randomSplit(Array(0.8, 0.2), 1030L)

    traindata.repartition(50).write.mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/home/cpc/zhj/ctr/dnn/data/traindata")
      //.save(s"/user/dnn_1537324485/cpc_data/ctr/traindata/$date")

    testdata.repartition(10).write.mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"/home/cpc/zhj/ctr/dnn/data/testdata")
    //.save(s"/user/dnn_1537324485/cpc_data/ctr/testdata/$date")

    /*//获取广告点击数据
    data.where("label = array(1,0)")
      .groupBy($"uid").agg(collect_set($"ideaid").alias("ideaids"))
      .coalesce(1)
      .write.mode("overwrite")
      .parquet(s"/home/cpc/zhj/ctr/dnn/data/click_data/$date")

    val ad_data = spark.read
      .parquet("/home/cpc/zhj/ctr/dnn/data/click_data/{" + getDays(date, 0, 3).mkString(",") + "}")
      .select($"uid", explode($"ideaids").alias("ideaid"))
      .groupBy("uid")
      .agg(collect_set("ideaid").alias("ideaids"))
*/

  }

  /**
    * 获取时间序列
    *
    * @param startdate : 日期
    * @param day1      ：日期之前day1天作为开始日期
    * @param day2      ：日期序列数量
    * @return
    */
  def getDays(startdate: String, day1: Int = 0, day2: Int): Seq[String] = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day1)
    var re = Seq(format.format(cal.getTime))
    for (i <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re
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

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @return
    */
  private def hash(prefix: String) = udf {
    num: String =>
      if (num != null) Murmur3Hash.stringHash(prefix + num) else Murmur3Hash.stringHash(prefix)
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
          if (seq != null) for (i <- seq) yield Murmur3Hash.stringHash(prefix + i)
          else Seq(Murmur3Hash.stringHash(prefix))
      }
      case "string" => udf {
        seq: Seq[String] =>
          if (seq != null) for (i <- seq) yield Murmur3Hash.stringHash(prefix + i)
          else Seq(Murmur3Hash.stringHash(prefix))
      }
    }
  }


}
