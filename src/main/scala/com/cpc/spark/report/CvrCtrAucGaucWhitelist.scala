package com.cpc.spark.report

import scala.collection.mutable

/*
  fym 190226.
  for cvr-ctr-auc-gauc-from-trident only.
 */

object CvrCtrAucGaucWhitelist {

  val wl = mutable.HashMap[String, Array[Int]](
    "cvr-qtt-all-portrait9" -> Array(2, 3),
    "noctr" -> Array(1, 2, 3),
    "qtt-api-cvr-dnn-rawid-v5" -> Array(1, 2, 3),
    "qtt-cvr-dnn-rawid-v1-180" -> Array(2, 3),
    "qtt-cvr-dnn-rawid-v2" -> Array(1, 3),
    "qtt-cvr-dnn-rawid-v3" -> Array(2, 3),
    "qtt-cvr-dnn-rawid-v4" -> Array(3),
    "novel-ctr-dnn-rawid-v7" -> Array(2, 3),
    "qtt-list-dnn-rawid-v4" -> Array(3)
  )
}
