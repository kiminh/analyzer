package com.cpc.spark.ml.train

import com.cpc.spark.common.SparkSessionTestWrapper
import mlmodel.mlmodel.Dict
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TestFtrlSnapshotId extends FlatSpec with Matchers with BeforeAndAfter
  with SparkSessionTestWrapper{

//  import spark.implicits._
//
//  "updateDict" should "update ftrl dict" in {
//    val ftrl = new Ftrl(10)
//    ftrl.dict = Dict(
//      advertiserid = Map(1 -> 1),
//      planid = Map(10 -> 10),
//      stringid = Map("a" -> 100)
//    )
//    val input = Array((2, "advertiser"), (11, "plan"))
//    val strInput = Array("b")
//    FtrlSnapshotId.updateDict(ftrl, input, strInput, 1000, 1000)
//    ftrl.dict.advertiserid.size should be (2)
//    ftrl.dict.planid.size should be (2)
//    ftrl.dict.advertiserid.contains(1) should be (true)
//    ftrl.dict.advertiserid.contains(2) should be (true)
//    ftrl.dict.planid.contains(10) should be (true)
//    ftrl.dict.planid.contains(11) should be (true)
//    ftrl.dict.stringid.contains("a") should be (true)
//    ftrl.dict.stringid.contains("b") should be (true)
//  }
//
//  "getAllIDFeatures" should "get ID from row" in {
//      val df = Seq(
//        (1, 2, Seq("a", "b"))
//      ).toDF("userid", "planid", "pkgs")
//    val ret = FtrlSnapshotId.getAllIDFeatures(df.collect().head)
//    val array = ret._1.sorted.toArray
//    val strArray = ret._2.toSet
//    array(0)._1 should be (1)
//    array(0)._2 should be ("advertiser")
//    array(1)._1 should be (2)
//    array(1)._2 should be ("plan")
//    strArray.contains("a_installed") should be (true)
//    strArray.contains("b_installed") should be (true)
//  }
//
//  "addIdFeatures" should "populate id features" in {
//    val df = Seq(
//      ("1 3", 1, 10, 15, Seq[String]()),
//      ("2 4", 0, 1000, 2000, Seq[String]())
//    ).toDF("leaf_features", "isclick", "userid", "planid", "pkgs")
//
//    val result = FtrlSnapshotId.addIdFeatures(df).collect()
//    result.head._1.label should be (1.0 +- 1e-6)
//    result.head._2.toSet should be (Set((10, "advertiser"), (15, "plan")))
//    result(1)._1.label should be (0.0 +- 1e-6)
//    result(1)._2.toSet should be (Set((1000, "advertiser"), (2000, "plan")))
//  }
}
