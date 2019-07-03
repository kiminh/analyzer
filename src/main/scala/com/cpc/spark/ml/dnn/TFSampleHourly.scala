package com.cpc.spark.ml.dnn

import com.cpc.spark.common.Murmur3Hash
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.array

/**
  * @author fym
  * @version created: 2019-06-18 17:07
  * @desc generate TFRecords. ov: zhj 181101
  */
object TFSampleHourly {

  Logger.getRootLogger.setLevel(Level.WARN)

  // multi hot 特征默认hash code
  private val default_hash = for (i <- 1 to 37) yield Seq((i - 1, 0, Murmur3Hash.stringHash64("m" + i, 0)))

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("[cpc-ml] dnn-sample-hourly-v4")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val date = args(0)
    val hour = args(1)

    // val train = getSample(spark, date, hour).persist()

    val queryToGo =
      s"""
         |select
         |  if(a.isclick>0, array(1, 0), array(0, 1)) as label
         |  , b.*
         |from dl_cpc.cpc_basedata_union_events a
         |left outer join dl_cpc.cpc_ml_nested_snapshot b
         |on a.searchid=b.searchid
         |  and a.ideaid=b.ideaid
         |  and a.day="$date"
         |  and a.hour=$hour
         |where b.day="$date"
         |  and b.hour=$hour
       """.stripMargin
    println(queryToGo)

    val sampleDataToGo = spark.sql(queryToGo)
      .select(array($"f1", $"f2", $"f3", $"f4", $"f5", $"f6", $"f7", $"f8", $"f9",
        $"f10", $"f11", $"f12", $"f13", $"f14", $"f15", $"f16", $"f17", $"f18", $"f19",
        $"f20", $"f21", $"f22", $"f23", $"f24", $"f25", $"f26", $"f27", $"f28", $"f29", $"f30", $"f31", $"f32", $"f33", $"f34", $"f35", $"f36", $"f37", $"f38", $"f39", $"f40", $"f41", $"f42", $"f43", $"f44", $"f45", $"f46", $"f47", $"f48", $"f49", $"f50", $"f51", $"f52", $"f53", $"f54", $"f55", $"f56", $"f57", $"f58", $"f59", $"f60", $"f61", $"f62", $"f63", $"f64", $"f65", $"f66", $"f67", $"f68", $"f69", $"ud0",$"ud1",$"ud2",$"ud3",$"ud4",$"ud5",$"ud6",$"ud7",$"ud8",$"ud9",$"ud10",$"ud11",$"ud12",$"ud13").alias("dense"),
        // mkSparseFeature($"apps", $"ideaids").alias("sparse"), $"label"
        // mkSparseFeature1($"m1").alias("sparse"), $"label"
        // mkSparseFeature_m($"raw_sparse").alias("sparse"),
        $"label"
      )
      .select(
        $"label",
        $"dense"
        //        $"sparse".getField("_1").alias("idx0"),
        //        $"sparse".getField("_2").alias("idx1"),
        //        $"sparse".getField("_3").alias("idx2"),
        //        $"sparse".getField("_4").alias("id_arr")
      )
      .rdd
      .zipWithUniqueId()
      .map { x =>
        (
          x._2,
          x._1.getAs[Seq[Int]]("label"),
          x._1.getAs[Seq[Seq[Long]]]("dense").flatten.toSeq
          /*,
          x._1.getAs[Seq[Int]]("idx0"), x._1.getAs[Seq[Int]]("idx1"),
          x._1.getAs[Seq[Int]]("idx2"), x._1.getAs[Seq[Long]]("id_arr")*/
        )
      }
      .toDF("sample_idx", "label", "dense" /*, "idx0", "idx1", "idx2", "id_arr"*/)

    val sampleDataCount = sampleDataToGo.count()

    println("total sample data => %d, positive => %.4f"
      .format(
        sampleDataCount,
        sampleDataToGo.where("label=array(1,0)").count.toDouble / sampleDataCount))

    sampleDataToGo
      .repartition(100)
      .write
      .mode("overwrite")
      .format("tfrecords")
      .option("recordType", "Example")
      .save(s"hdfs://emr-cluster/user/cpc/fym/snapshot-tfrecords-test/$date/$hour")

    sampleDataToGo.take(10).foreach(println)
    sampleDataToGo.unpersist()
  }

}
