
package com.cpc.ml.snapshot
import scala.collection.mutable
import scala.reflect.ClassTag


/**
  * 解析日志
  * @param searchid
  * @param media_appsid
  * @param uid
  * @param ideaid
  * @param userid
  * @param adslotid
  * @param adslot_type
  * @param content
  * @param feature_str
  * @param feature_int32
  * @param feature_int64
  * @param val_rec
  * @param day
  * @param hour
  * @param minute
  * @param pt
  */
case class CpcSnapshotEvent(
                                var searchid : String = "",
                                var media_appsid : String = "",
                                var uid : String = "",
                                var ideaid : Long = 0,
                                var userid : Long = 0,
                                var adslotid : Int = 0,
                                var adslot_type : Long = 0,
                                var content: collection.Map[String, Array[String]] = null, // <todo> int , float and / or string?
                                // feature stored as string.
                                var feature_str: collection.Map[String, Array[String]] = null,
                                // features stored as int32 values.
                                var feature_int32: collection.Map[String, Array[Int]] = null,
                                // features stored as int64 values.
                                var feature_int64: collection.Map[String, Array[Long]] = null,
                                var val_rec: collection.Map[String, Array[Int]] = null,
                                var day : String = "",
                                var hour : String = "",
                                var minute : String = "",
                                var pt : String = ""
                              ) {
  def setFeatures(
                   features: Array[String],
                   feature_str_offset: Array[Int],
                   feature_str_list: Array[String],
                   feature_int_offset: Array[Int],
                   feature_int_list: Array[Int],
                   feature_int64_offset: Array[Int],
                   feature_int64_list: Array[Long]
                 ): CpcSnapshotEvent = {

    val contentStr = mutable.Map[String, Array[String]]()
    val featureStr = mutable.Map[String, Array[String]]()
    val featureInt32 = mutable.Map[String, Array[Int]]()
    val featureInt64 = mutable.Map[String, Array[Long]]()
    val valRec = mutable.Map[String, Array[Int]]()

    features.foreach(f => {
      var name = f.toLowerCase()
      val index = features.indexOf(name)
      var str_content = getFeatureStrValue(index, feature_str_offset, feature_str_list)
      var list_32 = getFeatureInt32Value(index, feature_int_offset, feature_int_list)
      var list_64 = getFeatureInt64Value(index, feature_int64_offset, feature_int64_list)

      if (name.contains("val_rec_")) {
        val ideaidAndUnitid: String = name.replace("val_rec_", "")
        valRec.update(ideaidAndUnitid, list_32)
      } else if (name.contains("snapshot_postcali_vaule") ||
        name.contains("snapshot_expvalue") ||
        name.contains("snapshot_raw_expvalue") ||
        name.contains("snapshot_is_apicallback") ||
        name.contains("task_id")) {
        name = name.replaceAll("snapshot_", "")
        contentStr.update(name, str_content)
        featureInt32.update(name, list_32)
      } else if (name.contains("model_id") ||
        name.contains("model_name") ||
        name.contains("calibrations_key") ||
        name.contains("calibrations_md5")) {
        contentStr.update(name, str_content)
        featureStr.update(name, str_content)
      } else {
        name = name.replaceAll("snapshot_", "")
        contentStr.update(name, str_content)
        featureInt64.update(name, list_64)
      }
    })
    this.feature_str = featureStr.toMap
    this.feature_int32 = featureInt32.toMap
    this.feature_int64 = featureInt64.toMap
    this.content = contentStr.toMap
    this.val_rec = valRec.toMap

    this
  }

  def getFeatureInt64Value(index: Int,
                           feature_str_offset: Array[Int],
                           feature_str_list: Array[Long]): Array[Long] = {
    var app_fea_strs = new Array[Long](feature_str_list.size)
    if (index >= 0) {
      val left_offset = feature_str_offset(index)
      var right_offset = feature_str_offset.size
      if (index < feature_str_offset.size - 1)
        right_offset = feature_str_offset(index + 1)
      var i = left_offset
      while ( {
        i < right_offset
      }) {
        app_fea_strs = app_fea_strs :+ feature_str_list(i)
        {
          i += 1; i - 1
        }
      }
    }
    app_fea_strs
  }

  def getFeatureInt32Value(index: Int,
                           feature_str_offset: Array[Int],
                           feature_str_list: Array[Int]): Array[Int] = {
    var app_fea_strs = new Array[Int](feature_str_list.size)
    if (index >= 0) {
      val left_offset = feature_str_offset(index)
      var right_offset = feature_str_offset.size
      if (index < feature_str_offset.size - 1)
        right_offset = feature_str_offset(index + 1)
      var i = left_offset
      while ( {
        i < right_offset
      }) {
        app_fea_strs = app_fea_strs :+ feature_str_list(i)
        {
          i += 1; i - 1
        }
      }
    }
    app_fea_strs
  }

  def getFeatureStrValue(index: Int,
                           feature_str_offset: Array[Int],
                           feature_str_list: Array[String]): Array[String] = {
    var app_fea_strs = new Array[String](feature_str_list.length)
    if (index >= 0) {
      val left_offset = feature_str_offset(index)
      var right_offset = feature_str_offset.size
      if (index < feature_str_offset.size - 1)
        right_offset = feature_str_offset(index + 1)
      var i = left_offset
      while ( {
        i < right_offset
      }) {
        app_fea_strs = app_fea_strs :+ feature_str_list(i);
        {
          i += 1; i - 1
        }
      }
    }
    app_fea_strs
  }

}
