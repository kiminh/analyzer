package com.cpc.spark.udfs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

object Udfs_wj{
  def udfOcpcLogExtractCPA() = udf((valueLog: String) => {
    val logs = valueLog.split(",")
    logs(2)
  })
}
