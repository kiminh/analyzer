package com.cpc.spark.ml.dnn.trash

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
@deprecated
object DNNCtrDataPrepare {
  def main(args: Array[String]): Unit = {

    val date = args(0) //"2018-09-26"

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
         |where date > '${getDay(date, 4)}'
         |  and date < '$date'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
         |group by uid
      """.stripMargin)

    //获取最近4到7天点击数据
    val click_data2 = spark.sql(
      s"""
         |select uid,collect_set(ideaid) as ideaids
         |from dl_cpc.ml_cvr_feature_v1
         |where date > '${getDay(date, 8)}'
         |  and date <= '${getDay(date, 4)}'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
         |group by uid
      """.stripMargin)

    val uid_show = spark.sql(
      s"""
        |select uid,collect_set(ideaid) as show_ads
        |from dl_cpc.ml_ctr_feature_v1
        |where date = '$date'
        |  and ideaid > 0
        |  and adslot_type = 1
        |  and media_appsid in ('80000001','80000002')
        |group by uid
      """.stripMargin)

    val mkSparseFeature = udf {
      (apps: Seq[Long], ideaids: Seq[Long]) =>
        val a = apps.zipWithIndex.map(x => (0, x._2, x._1))
        val b = ideaids.zipWithIndex.map(x => (1, x._2, x._1))
        val c = (a ++ b).map(x => (0, x._1, x._2, x._3))
        (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
    }

    val mkSparseFeature1 = udf {
      apps: Seq[Long] =>
        val c = apps.zipWithIndex.map(x => (0, 0, x._2, x._1))
        (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
    }

    //合并数据
    val data = spark.sql(
      s"""
         |select uid,hour,sex,age,os,network,city,
         |      adslotid,phone_level,adclass,
         |      adtype,planid,unitid,ideaid,
         |      if(label>0, array(1,0), array(0,1)) as label
         |from dl_cpc.ml_ctr_feature_v1
         |where date = '$date'
         |  and ideaid > 0
         |  and adslot_type = 1
         |  and media_appsid in ('80000001','80000002')
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
      """.stripMargin)
      .join(app_data, Seq("uid"), "leftouter")
      .join(click_data1, Seq("uid"), "leftouter")
      .join(uid_show, Seq("uid"), "leftouter")
      .where("size(show_ads) >= 10 or size(ideads) > 0")
      .select($"label",
        hash("uid")($"uid").alias("uid1"),
        hash("age")($"age").alias("age1"),
        hash("hour")($"hour").alias("hour1"),
        hash("sex")($"sex").alias("sex1"),
        hash("os")($"os").alias("os1"),
        hash("network")($"network").alias("network1"),
        hash("city")($"city").alias("city1"),
        hash("adslotid")($"adslotid").alias("adslotid1"),
        hash("phone_level")($"phone_level").alias("pl1"),
        hash("adclass")($"adclass").alias("adclass1"),
        hash("adtype")($"adtype").alias("adtype1"),
        hash("planid")($"planid").alias("planid1"),
        hash("unitid")($"unitid").alias("unitid1"),
        hash("ideaid")($"ideaid").alias("ideaid1"),
        hashSeq("app", "string")($"pkgs").alias("apps1"),
        hashSeq("ideaid", "int")($"ideaids").alias("ideaids1"))
      .select(array($"uid1", $"hour1", $"age1", $"sex1", $"os1", $"network1", $"city1", $"adslotid1", $"pl1",
        $"adclass1", $"adtype1", $"planid1", $"unitid1", $"ideaid1").alias("dense"),
        //mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
        mkSparseFeature1($"apps1").alias("sparse"), $"label"
      )
      //生成带index的目标数据
      /*
            .rdd.zipWithIndex
            .map { x =>
              val sparse = x._1.getAs[(Seq[Int], Seq[Int], Seq[Int], Seq[Long])]("sparse")
              (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Int]]("dense"),
                sparse._1, sparse._2, sparse._3, sparse._4)
            }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
      */
      .select(
      $"label",
      $"dense",
      $"sparse".getField("_1").alias("idx0"),
      $"sparse".getField("_2").alias("idx1"),
      $"sparse".getField("_3").alias("idx2"),
      $"sparse".getField("_4").alias("id_arr"))

    val Array(traindata, testdata) = data.randomSplit(Array(0.99, 0.01), 1030L)

    //println("traindata count = " + traindata.count)
    //traindata.show

    //println("testdata count = " + testdata.count)
    //testdata.show

    //traindata.write.mode("overwrite").parquet("/home/cpc/zhj/ctr/dnn/data/test")
    testdata.persist()

    println("train data no app num ：" + traindata.where("size(sparse._4)=0").count)
    println("test data no app num ：" + testdata.where("size(sparse._4)=0").count)

    println("训练数据：total = %d, 正比例 = %.4f".format(traindata.count,
      traindata.where("label=array(1,0)").count.toDouble / traindata.count))

    println("测试数据：total = %d, 正比例 = %.4f".format(testdata.count,
      testdata.where("label=array(1,0)").count.toDouble / testdata.count))

    traindata.rdd.zipWithIndex().map { x =>
      (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
        x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
        x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
    }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
      .repartition(100).write.mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnn/traindata")

    testdata.rdd.zipWithIndex().map { x =>
      (x._2, x._1.getAs[Seq[Int]]("label"), x._1.getAs[Seq[Long]]("dense"),
        x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
        x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr"))
    }.toDF("sample_idx", "label", "dense", "idx0", "idx1", "idx2", "id_arr")
      .repartition(100).write.mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save("/user/cpc/dw/dnn/testdata")

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
      if (num != null) Murmur3Hash.stringHash64(prefix + num, 0) else Murmur3Hash.stringHash64(prefix, 0)
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
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 1030)
          else Seq(Murmur3Hash.stringHash64(prefix, 1030))
          re.slice(0, 1000)
      }
      case "string" => udf {
        seq: Seq[String] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 1030)
          else Seq(Murmur3Hash.stringHash64(prefix, 1030))
          re.slice(0, 1000)
      }
    }
  }
}
