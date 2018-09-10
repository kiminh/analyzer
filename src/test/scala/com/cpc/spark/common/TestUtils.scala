package com.cpc.spark.common

import org.scalatest._

class TestUtils extends FlatSpec with Matchers {

  "getCtrModelIdFromExpTags" should "extract model id correctly" in {
    var model = Utils.getCtrModelIdFromExpTags("a=b,ctrmodel=modelA")
    model should be ("modelA")
    model = Utils.getCtrModelIdFromExpTags("a=b,ctrmodel=modelB,bla=bla")
    model should be ("modelB")
    model = Utils.getCtrModelIdFromExpTags("ctrmodel=modelC")
    model should be ("modelC")
  }

  it should "return <undefined> if not ctrmodel is found" in {
    val model = Utils.getCtrModelIdFromExpTags("")
    model should be ("<undefined>")
  }
}