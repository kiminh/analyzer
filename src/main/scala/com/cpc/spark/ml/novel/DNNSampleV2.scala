package com.cpc.spark.ml.novel

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.process._

/**
  * 准备tfrecord格式训练和测试数据给tensorflow
  * created time : 2018/11/08 14:12
  *
  * @author zhj
  * @version 1.0
  *
  */
class DNNSampleV2(spark: SparkSession, trDate: String, trPath: String,
                  teDate: String, tePath: String, adslotidChoose: String) extends Serializable {

  def saveTrain(p: String = trPath, num_partitions: Int = 1000, adslotidChoose: String): Unit = {
    println("Starting preparing data for train")

    /*val st = traindata.sample(withReplacement = true, 0.01).count

    println(s"训练数据总量：${st * 100}")*/

    val data = getTrainSample(spark, trDate, adslotidChoose)
      .repartition(if (trDate.length == 13) 100 else num_partitions)

    data.write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"$p/dnntrain-$trDate")

    //确认样本数
//    if (trDate.length == 13) {
      val count = data.count()
    println("训练数据：total = %d, 正比例 = %.4f".format(count, data.where("label=array(1,0)").count.toDouble / count))
      s"echo $count" #> new File("count") !

      s"hadoop fs -put count $p/dnntrain-$trDate/" !
//    }

    println(s"DONE : Saving train file to $trPath/dnntrain-$trDate")
  }

  def saveTest(gauc: Boolean = false, p: String = tePath, num_partitions: Int = 100): Unit = {
    import spark.implicits._

    println("Start preparing data for test")

    var path: String = ""
    if (gauc) {
      path = s"$p/dnntest-gauc-$teDate"
      getTestSample4Gauc(spark, teDate)
        .repartition(num_partitions, $"sample_idx")
        .write
        .mode("overwrite")
        .format("tfrecords")
        .option("recordType", "Example")
        .save(path)
    } else {
      path = s"$p/dnntest-$teDate"
      getTestSample(spark, teDate)
        .repartition(num_partitions)
        .write
        .mode("overwrite")
        .format("tfrecords")
        .option("recordType", "Example")
        .save(path)
    }

    println(s"Done : Saving test file to $path")
  }

  /**
    * 获取训练数据的dataframe
    *
    * @param spark ：sparksession
    * @param date  ：需要准备的训练数据的日期
    * @return
    */
  def getTrainSample(spark: SparkSession, date: String, adslotidChoose: String): DataFrame = {
    println("==========================")
    println("YOU MUST OVERWRITE THIS 'getTrainSample' METHOD!!!")
    println("==========================")
    System.exit(1)
    spark.sql("")
  }

  /**
    * 准备测试数据
    *
    * @param spark   ：sparksession
    * @param date    ：日期
    * @param percent ：选取比例，默认0.05
    * @return
    */
  def getTestSample(spark: SparkSession, date: String,
                    percent: Double = 0.05): DataFrame = {
    println("==========================")
    println("YOU MUST OVERWRITE THIS 'getTestSamle' METHOD!!!")
    println("==========================")
    System.exit(1)
    spark.sql("")
  }

  /**
    * 准备测试数据
    *
    * @param spark   ：sparksession
    * @param date    ：日期
    * @param percent ：选取比例，默认0.05
    * @return
    */
  def getTestSample4Gauc(spark: SparkSession, date: String,
                         percent: Double = 0.05): DataFrame = {
    println("==========================")
    println("YOU MUST OVERWRITE THIS 'getTestSamle4Gauc' METHOD!!!")
    println("==========================")
    System.exit(1)
    spark.sql("")
  }

  /**
    * 获取时间序列
    *
    * @param startdate : 日期
    * @param day1      ：日期之前day1天作为开始日期
    * @param day2      ：日期序列数量
    * @return
    */
  def getDays(startdate: String, day1: Int = 0, day2: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day1)
    var re = Seq(format.format(cal.getTime))
    for (_ <- 1 until day2) {
      cal.add(Calendar.DATE, -1)
      re = re :+ format.format(cal.getTime)
    }
    re.mkString("','")
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

  def getUidApp(spark: SparkSession, date: String): DataFrame = {
    import spark.implicits._
    spark.sql(
      """
        |select * from dl_cpc.cpc_user_installed_apps where `load_date` = "%s"
      """.stripMargin.format(date)).rdd
      .map(x => (x.getAs[String]("uid"), x.getAs[Seq[String]]("pkgs")))
      .reduceByKey(_ ++ _)
      .map(x => (x._1, x._2.distinct))
      .toDF("uid", "pkgs")
  }

  /**
    * 获取hash code
    *
    * @param prefix ：前缀
    * @return
    */
  def hash(prefix: String) = udf {
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
  def hashSeq(prefix: String, t: String) = {
    t match {
      case "int" => udf {
        seq: Seq[Int] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
      case "string" => udf {
        seq: Seq[String] =>
          val re = if (seq != null && seq.nonEmpty) for (i <- seq) yield Murmur3Hash.stringHash64(prefix + i, 0)
          else Seq(Murmur3Hash.stringHash64(prefix, 0))
          re.slice(0, 1000)
      }
    }
  }

  /**
    * 更具指定条件使用默认值更换dense特征中的值
    *
    * @param p :位置 0 ~ length-1
    * @param d :默认值
    * @return
    */
  def getNewDense(p: Int, d: Long) = udf {
    (dense: Seq[Long], f: Boolean) =>
      if (f) (dense.slice(0, p) :+ d) ++ dense.slice(p + 1, 1000) else dense
  }

  private val default_hash = for (i <- 1 to 100) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def mkSparseFeature_m = udf {
    features: Seq[Seq[Long]] =>
      var i = 0
      var re = Seq[(Int, Int, Long)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null && feature.nonEmpty) feature.zipWithIndex.map(x => (i, x._2, x._1)) else default_hash(i))
        i = i + 1
      }
      val c = re.map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

  def mkSparseFeature(default: Seq[(Int, Int, Long)]) = udf {
    features: Seq[Seq[Long]] =>
      var i = 0
      var re = Seq[(Int, Int, Long)]()
      for (feature <- features) {
        re = re ++
          (if (feature != null && feature.nonEmpty) feature.zipWithIndex.map(x => (i, x._2, x._1)) else Seq(default(i)))
        i = i + 1
      }
      val c = re.map(x => (0, x._1, x._2, x._3))
      (c.map(_._1), c.map(_._2), c.map(_._3), c.map(_._4))
  }

  //获取小于当前小时的指定Seq【id】的hash值
  def filterHash(index: Int) = udf {
    (hour: String, values: Seq[String]) =>
      val re = if (values != null && values.exists(_ < hour)) {
        values.filter(_ < hour).map(x => x.split(":")(1).toLong)
      }
      else Seq(Murmur3Hash.stringHash64("m" + index, 0))
      re.slice(0, 1000)
  }

  def filterHash1(index: Int) = udf {
    (hour: String, values: Seq[String]) =>
      val re = if (values != null && values.exists(_.split(":")(0) <= hour)) {
        values.filter(_.split(":")(0) <= hour).map(x => x.split(":")(1).toLong)
      }
      else Seq(Murmur3Hash.stringHash64("m" + index, 0))
      re.slice(0, 1000)
  }

  def filterHash2(index: Int) = udf {
    (hour: String, values: Seq[String]) =>
      var re = Seq(Murmur3Hash.stringHash64("m" + index, 0))
      var h = hour.toInt
      var re1 = Seq[Long]()
      if (h > 2) {
        if (values != null) {
          re1 = values.filter { x =>
            x.split(":")(0).toInt < (h - 2)
          }
            .map { x =>
              x.split(":")(1).toLong
            }
        }
      }
      if (re1.nonEmpty) re1.distinct.slice(0, 1000) else re
  }

  def filterHash3 = udf {
    (hour: String, adclass: Int, values: Seq[String]) =>
      val re = if (values != null && values.nonEmpty) {
        val cnt = values.map(_.split(":"))
          .filter(_ (0) < hour)
          .count(_ (1).toInt == adclass)
        if (cnt > 0) Murmur3Hash.stringHash64("m171", 0) else Murmur3Hash.stringHash64("m170", 0)
      } else Murmur3Hash.stringHash64("m170", 0)

  }

  def findHash(m: Map[String, Long], default: Long) = udf {
    values: Seq[String] =>
      if (values != null && values.nonEmpty) {
        for (v <- values.slice(0, 1000)) yield m.getOrElse(v, 0L)
      }
      else Seq(default)
  }
}
