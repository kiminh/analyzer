package com.cpc.spark.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Udfs_wj{
  def udfMode1OcpcLogExtractCPA1() = udf((valueLog: String) => {
    val logs = valueLog.split(",")
    logs(2)
  })

  def udfModelOcpcLogExtractCPA2() = udf((valueLog: String) => {
    val logs = valueLog.split(",")
    logs(2)
  })
}
