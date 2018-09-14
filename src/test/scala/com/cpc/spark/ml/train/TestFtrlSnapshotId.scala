package com.cpc.spark.ml.train

import com.cpc.spark.common.SparkSessionTestWrapper
import mlmodel.mlmodel.Dict
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class TestFtrlSnapshotId extends FlatSpec with Matchers with BeforeAndAfter
  with SparkSessionTestWrapper{

  import spark.implicits._

  "updateDict" should "update ftrl dict" in {
    val ftrl = new Ftrl(10)
    ftrl.dict = Dict(
      advertiserid = Map(1 -> 1),
      planid = Map(10 -> 10)
    )
    val input = Array((2, "advertiser"), (11, "plan"))
    FtrlSnapshotId.updateDict(ftrl, input)
    ftrl.dict.advertiserid.size should be (2)
    ftrl.dict.planid.size should be (2)
    ftrl.dict.advertiserid.contains(1) should be (true)
    ftrl.dict.advertiserid.contains(2) should be (true)
    ftrl.dict.planid.contains(10) should be (true)
    ftrl.dict.planid.contains(11) should be (true)
  }

  "getAllIDFeatures" should "get ID from row" in {
      val df = Seq(
        (1, 2)
      ).toDF("userid", "planid")
    val ret = FtrlSnapshotId.getAllIDFeatures(df.collect().head)
    val array = ret.sorted.toArray
    array(0)._1 should be (1)
    array(0)._2 should be ("advertiser")
    array(1)._1 should be (2)
    array(1)._2 should be ("plan")
  }

  "addIdFeatures" should "populate id features" in {
    val df = Seq(
      ("1 3", 1, 10, 15),
      ("2 4", 0, 1000, 2000)
    ).toDF("leaf_features", "isclick", "userid", "planid")

    val result = FtrlSnapshotId.addIdFeatures(df).collect()
    result.head._1.features.toSparse.indices.toSet should be (Set(1, 3, 1445119, 1492022))
    result.head._1.label should be (1.0 +- 1e-6)
    result.head._2.toSet should be (Set((10, "advertiser"), (15, "plan")))
    result(1)._1.features.toSparse.indices.toSet should be (Set(2, 4, 844082, 968991))
    result(1)._1.label should be (0.0 +- 1e-6)
    result(1)._2.toSet should be (Set((1000, "advertiser"), (2000, "plan")))
  }
}
