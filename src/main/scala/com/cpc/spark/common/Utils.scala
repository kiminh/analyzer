package com.cpc.spark.common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by roydong on 09/08/2017.
  */
object Utils {

  def deleteHdfs(path: String): Boolean = {
    val conf = new Configuration()
    val p = new Path(path)
    val hdfs = FileSystem.get(conf)
    hdfs.delete(p, true)
  }

}
