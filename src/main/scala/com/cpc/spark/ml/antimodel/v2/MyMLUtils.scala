package com.cpc.spark.ml.antimodel.v2

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object MyMLUtils  {

  def loadLibSVMFile(sc: SparkContext, path: String): RDD[(String, LabeledPoint)] =
    loadLibSVMFile(sc, path, -1)

  def loadLibSVMFile(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int): RDD[(String, LabeledPoint)] =
    loadLibSVMFile(sc, path, numFeatures, sc.defaultMinPartitions)

  def loadLibSVMFile(
                      sc: SparkContext,
                      path: String,
                      numFeatures: Int,
                      minPartitions: Int): RDD[(String, LabeledPoint)] = {
    val parsed = parseLibSVMFile(sc, path, minPartitions)

    // Determine number of features.
    val d = if (numFeatures > 0) {
      numFeatures
    } else {
      parsed.persist(StorageLevel.MEMORY_ONLY)
      computeNumFeatures(parsed)
    }

    parsed.map { case (uid, label, indices, values) =>
      (uid, LabeledPoint(label, Vectors.sparse(d, indices, values)))
    }
  }


  private def parseLibSVMFile(
                                      sc: SparkContext,
                                      path: String,
                                      minPartitions: Int): RDD[(String, Double, Array[Int], Array[Double])] = {
    sc.textFile(path, minPartitions)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#") || line.split(",").length != 2))
      .map(parseLibSVMRecord).filter(x => x != null)
  }

  private def parseLibSVMRecord(line: String): (String, Double, Array[Int], Array[Double]) = {
    try{
      val items2 = line.split(',')
      val uid = items2(0)
      val items = items2(1).split(' ')
      val label = items.head.toDouble
      val (indices, values) = items.tail.filter(_.nonEmpty).map { item =>
        val indexAndValue = item.split(':')
        val index = indexAndValue(0).toInt - 1 // Convert 1-based indices to 0-based.
      val value = indexAndValue(1).toDouble
        (index, value)
      }.unzip
      // check if indices are one-based and in ascending order
      var previous = -1
      var i = 0
      val indicesLength = indices.length
      while (i < indicesLength) {
        val current = indices(i)
        require(current > previous, s"indices should be one-based and in ascending order;"
          + " found current=$current, previous=$previous; line=\"$line\"")
        previous = current
        i += 1
      }
      (uid, label, indices.toArray, values.toArray)
    }catch{
      case e: Exception =>
        println("exception: " + line)
        null
    }
  }

  private def computeNumFeatures(rdd: RDD[(String, Double, Array[Int], Array[Double])]): Int = {
    rdd.map { case (uid, label, indices, values) =>
      indices.lastOption.getOrElse(0)
    }.reduce(math.max) + 1
  }
}
