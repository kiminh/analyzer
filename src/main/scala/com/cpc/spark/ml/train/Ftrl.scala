package com.cpc.spark.ml.train

import java.io._
import java.util.Date

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import com.alibaba.fastjson.{JSON, JSONObject}
import com.cpc.spark.common.Utils
import com.cpc.spark.ml.ftrl.FtrlSerializable
import com.cpc.spark.qukan.utils.RedisUtil
import com.google.protobuf.CodedInputStream
import com.redis.RedisClient
import mlmodel.mlmodel._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


class Ftrl(size: Int) {

  var alpha: Double = 0.01
  var beta: Double = 1.0
  var L1: Double = 0.0
  var L2: Double = 0.0
  var n, z, w = new Array[Double](size)
  var nDict, zDict, wDict = mutable.Map[Int, Double]()
  var dict: Dict = Dict()

  val TOLERANCE: Double = 1e-6d


  def logLoss(y: Double, p: Double): Double = {
    if (y == 1.0d) {
      return -1.0 * Math.log(Math.max(p, TOLERANCE))
    }
    else if (y == 0.0d) {
      return -1.0 * Math.log(Math.max(1d - p, 1d - TOLERANCE))
    }
    0d
  }

  def toJsonString: String = {
    val json = new JSONObject()
    json.put("alpha", alpha)
    json.put("beta", beta)
    json.put("L1", L1)
    json.put("L2", L2)

    json.put("n", n.mkString(" "))
    json.put("z", z.mkString(" "))
    json.put("w", w.mkString(" "))
    json.put("dict_advertiser", mapToJson(dict.advertiserid))
    json.put("dict_plan", mapToJson(dict.planid))
    json.put("dict_idea", mapToJson(dict.ideaid))
    json.put("dict_string", mapToJson(dict.stringid))
    json.toJSONString()
  }

  def fromJsonString(jsonString: String): Unit = {
    val json = JSON.parseObject(jsonString)
    w = json.getString("w").split(" ").map(_.toDouble)
    n = json.getString("n").split(" ").map(_.toDouble)
    z = json.getString("z").split(" ").map(_.toDouble)
    alpha = json.getDoubleValue("alpha")
    beta = json.getDoubleValue("beta")
    L1 = json.getDoubleValue("L1")
    L2 = json.getDoubleValue("L2")
    dict = Dict(
      planid = jsonToIntMap(json.getJSONObject("dict_plan")),
      advertiserid = jsonToIntMap(json.getJSONObject("dict_advertiser")),
      ideaid = jsonToIntMap(json.getJSONObject("dict_idea")),
      stringid = jsonToStrMap(json.getJSONObject("dict_string"))
    )
  }

  def mapToJson[T, U](map: Map[T, U]): JSONObject = {
    val json = new JSONObject()
    map.foreach(x => json.put(x._1.toString, x._2))
    return json
  }

  def jsonToIntMap(json: JSONObject): Map[Int, Int] = {
    val map = mutable.Map[Int, Int]()
    if (json == null) {
      return map.toMap
    }
    val keyIt = json.keySet().iterator()
    while (keyIt.hasNext) {
      val key = keyIt.next()
      map.put(key.toInt, json.getIntValue(key))
    }
    return map.toMap
  }

  def jsonToStrMap(json: JSONObject): Map[String, Int] = {
    val map = mutable.Map[String, Int]()
    if (json == null) {
      return map.toMap
    }
    val keyIt = json.keySet().iterator()
    while (keyIt.hasNext) {
      val key = keyIt.next()
      map.put(key.toString, json.getIntValue(key))
    }
    return map.toMap
  }

  def print() = {
    println("n:")
    println(n.mkString(" "))
    println("z:")
    println(z.mkString(" "))
    println("w:")
    println(w.mkString(" "))
    println("alpha:")
    println(alpha)
    println("beta:")
    println(beta)
    println("L1:")
    println(L1)
    println("L2:")
    println(L2)
    println("dict_advertiser (top 5):")
    println(dict.advertiserid.toArray.slice(0, Math.min(5, dict.advertiserid.size)))
    println("dict_idea (top 5):")
    println(dict.ideaid.toArray.slice(0, Math.min(5, dict.ideaid.size)))

  }

  def predict(x: Array[Int]): Double = {
    var wTx = 0.0

    x foreach { x =>
      val sign = if (z(x) < 0) -1.0 else 1.0

      if (sign * z(x) <= L1)
        w(x) = 0.0
      else
        w(x) = (sign * L1 - z(x)) / ((beta + math.sqrt(n(x))) / alpha + L2)

      wTx = wTx + w(x)
    }
    return 1.0 / (1.0 + math.exp(-wTx))
  }

  def sigmoid(x: Double): Double = {
    if (x <= -35.0d) {
      return 0.000000000000001d;
    } else if (x >= 35.0d) {
      return 0.999999999999999d;
    }
    return 1.0d / (1.0d + math.exp(-x))
  }

  def predictWithDict(x: Array[Int]): Double = {
    var wTx = 0.0

    x foreach { x =>
      val sign = if (zDict.getOrElse(x, 0d) < 0) -1.0 else 1.0

      if (sign * zDict.getOrElse(x, 0d) <= L1) {
        wDict.remove(x)
      }
      else {
        wDict.put(x, (sign * L1 - zDict.getOrElse(x, 0d)) / ((beta + math.sqrt(nDict.getOrElse(x, 0d))) / alpha + L2))
      }
      wTx = wTx + wDict.getOrElse(x, 0d)
    }
    return sigmoid(wTx)
  }

  def predictWithSubDict(x: Array[Int], nMap: mutable.Map[Int, Double], zMap: mutable.Map[Int, Double],
                         wMap: mutable.Map[Int, Double]): Double = {
    var wTx = 0.0
    var mw = 0.0
    x foreach { x =>
      val sign = if (zMap.getOrElse(x, 0d) < 0) -1.0 else 1.0

      if (sign * zMap.getOrElse(x, 0d) <= L1) {
        mw = 0.0
      }
      else {
        mw = (sign * L1 - zMap.getOrElse(x, 0d)) / ((beta + math.sqrt(nMap.getOrElse(x, 0d))) / alpha + L2)
      }
      wMap.put(x, mw)
      wTx = wTx + mw
    }
    return sigmoid(wTx)
  }

  def predictNoUpdateWithDict(x: Array[Int]): Double = {
    var wTx = 0.0

    x foreach { x =>
      wTx = wTx + wDict.getOrElse(x, 0d)
    }
    return 1.0 / (1.0 + math.exp(-wTx))
  }

  def update(x: Array[Int], p: Double, y: Double): Unit = {
    val g = p - y

    x foreach { x =>
      val sigma = (math.sqrt(n(x) + g * g) - math.sqrt(n(x))) / alpha
      z(x) = z(x) + g - sigma * w(x)
      n(x) = n(x) + g * g
    }
  }

  def updateWithDict(x: Array[Int], p: Double, y: Double): Unit = {
    val g = p - y

    x foreach { x =>
      val sigma = (math.sqrt(nDict.getOrElse(x, 0d) + g * g) - math.sqrt(nDict.getOrElse(x, 0d))) / alpha
      zDict.put(x, zDict.getOrElse(x, 0d) + g - sigma * wDict.getOrElse(x, 0d))
      nDict.put(x, nDict.getOrElse(x, 0d) + g * g)
    }
  }

  def updateWithSubDict(x: Array[Int], p: Double, y: Double,
                        nMap: mutable.Map[Int, Double], zMap: mutable.Map[Int, Double],
                        wMap: mutable.Map[Int, Double]): Unit = {
    val g = p - y

    x foreach { x =>
      val sigma = (math.sqrt(nMap.getOrElse(x, 0d) + g * g) - math.sqrt(nMap.getOrElse(x, 0d))) / alpha
      zMap.put(x, zMap.getOrElse(x, 0d) + g - sigma * wMap.getOrElse(x, 0d))
      nMap.put(x, nMap.getOrElse(x, 0d) + g * g)
    }
  }

  def predictAndAuc(session: SparkSession, instances: Array[LabeledPoint]): Double = {
    val listBuffer = new ListBuffer[(Double, Double)]
    for (instance <- instances) {
      val x = instance.features.toSparse.indices
      val pre = predict(x)
      listBuffer.append((pre, instance.label))
    }
    val metrics = new BinaryClassificationMetrics(session.sparkContext.parallelize(listBuffer))
    val auc = metrics.areaUnderROC()
    return auc
  }

  def predictAndAucWithDict(session: SparkSession, instances: Array[(Array[Int], Double)]): Double = {
    val listBuffer = new ListBuffer[(Double, Double)]
    for (instance <- instances) {
      val x = instance._1
      val pre = predictWithDict(x)
      listBuffer.append((pre, instance._2))
    }
    val metrics = new BinaryClassificationMetrics(session.sparkContext.parallelize(listBuffer))
    val auc = metrics.areaUnderROC()
    return auc
  }

  def predictAndAucWithSubDict(session: SparkSession, instances: Array[(Array[Int], Double)],
                               nMap: mutable.Map[Int, Double], zMap: mutable.Map[Int, Double],
                               wMap: mutable.Map[Int, Double]): Double = {
    val listBuffer = new ListBuffer[(Double, Double)]
    for (instance <- instances) {
      val x = instance._1
      val pre = predictWithSubDict(x, nMap, zMap, wMap)
      listBuffer.append((pre, instance._2))
    }
    val metrics = new BinaryClassificationMetrics(session.sparkContext.parallelize(listBuffer))
    val auc = metrics.areaUnderROC()
    return auc
  }

  def trainWithDict(spark: SparkSession, data: Array[(Array[Int], Double)]): Unit = {
    var posCount = 0
    val res = shuffle(data)
    println(s"before training auc on test set: ${predictAndAucWithDict(spark, res)}")
    var logLossSum = 0d
    for (p <- res) {
      val x = p._1
      val pre = predictWithDict(x)
      updateWithDict(x, pre, p._2)
      if (p._2 > 0) {
        posCount += 1
      }
      logLossSum += logLoss(p._2, pre)
    }
    println(s"logloss=${logLossSum/res.length}")
    println(s"posCount=$posCount, totalCount=${res.length}")
    val afterAUC = predictAndAucWithDict(spark, res)
    println(s"after training auc: $afterAUC")
  }

  def trainWithSubDict(spark: SparkSession, data: Array[(Array[Int], Double)],
                       nMap: mutable.Map[Int, Double], zMap: mutable.Map[Int, Double]): mutable.Map[Int, Double] = {
    var posCount = 0
    val res = shuffle(data)
    val wMap = mutable.Map[Int, Double]()
    println(s"before training auc on test set: ${predictAndAucWithSubDict(spark, res, nMap, zMap, wMap)}")
    var logLossSum = 0d
    for (p <- res) {
      val x = p._1
      val pre = predictWithSubDict(x, nMap, zMap, wMap)
      updateWithSubDict(x, pre, p._2, nMap, zMap, wMap)
      if (p._2 > 0) {
        posCount += 1
      }
      logLossSum += logLoss(p._2, pre)
    }
    println(s"logloss=${logLossSum/res.length}")
    println(s"posCount=$posCount, totalCount=${res.length}")
    val afterAUC = predictAndAucWithSubDict(spark, res, nMap, zMap, wMap)
    println(s"after training auc: $afterAUC")
    return wMap
  }

  def trainMoreEpochsWithDict(spark: SparkSession, data: RDD[(Array[Int], Double)]): Unit = {
    val epoch = 5
    var res = shuffle(data.collect())
    println(s"before training auc on test set: ${predictAndAucWithDict(spark, res)}")
    for (_ <- 1 to epoch) {
      var logLossSum = 0d
      for (p <- res) {
        val x = p._1
        val pre = predictWithDict(x)
        updateWithDict(x, pre, p._2)
        logLossSum += logLoss(p._2, pre)
      }
      println(s"logloss=${logLossSum/res.length}")
      res = shuffle(data.collect())
    }
    val afterAUC = predictAndAucWithDict(spark, res)
    println(s"after training auc: $afterAUC")
  }

  def train(spark: SparkSession, data: RDD[LabeledPoint]): Unit = {
    var posCount = 0
    val res = shuffle(data.collect())

    val beforeAUC = predictAndAuc(spark, res)
    println(s"before training auc: $beforeAUC")
    var totalPredict = 0.0
    for (p <- res) {
      val x = p.features.toSparse.indices
      val pre = predict(x)
      totalPredict = totalPredict + pre
      update(x, pre, p.label)
      if (p.label > 0) {
        posCount += 1
      }
    }
    val averagePredict: Double = totalPredict / res.length
    println(s"posCount=$posCount, totalCount=${res.size}")
    val afterAUC = predictAndAuc(spark, res)
    println(s"after training auc=$afterAUC")
    println(s"average prediction = $averagePredict")
  }

  def shuffle[T](array: Array[T]): Array[T] = {
    val rnd = new java.util.Random
    for (n <- Iterator.range(array.length - 1, 0, -1)) {
      val k = rnd.nextInt(n + 1)
      val t = array(k); array(k) = array(n); array(n) = t
    }
    return array
  }
}

object Ftrl {

  def getNZFromModel(data: Array[(Array[Int], Double)], ftrl: Ftrl): (mutable.Map[Int, Double], mutable.Map[Int, Double]) = {
    val nMap = mutable.Map[Int, Double]()
    val zMap = mutable.Map[Int, Double]()
    data.foreach( x => {
      x._1.foreach( i => {
        nMap.put(i, ftrl.nDict.getOrElse(i, 0))
        zMap.put(i, ftrl.zDict.getOrElse(i, 0))
      })
    })
    println(s"batch model size: ${nMap.size}")
    (nMap, zMap)
  }

  def getNZFromRedis(data: Array[(Array[Int], Double)], redisDB: Int): (mutable.Map[Int, Double], mutable.Map[Int, Double]) = {
    val keySet = mutable.Set[Int]()
    data.foreach( x => {
      x._1.foreach( i => {
        keySet.add(i)
      })
    })
    println(s"batch model size: ${keySet.size}")
    RedisUtil.getNZFromRedis(redisDB, keySet)
  }

  def getModelFromHDFS(startFresh: Boolean, key: String, ctx: SparkSession, size: Int): Ftrl = {
    val ftrl = new Ftrl(size)
    if (startFresh) {
      println("new ftrl")
      return ftrl
    }
    val json = ctx.sparkContext.textFile(key).collect().mkString("\n")
    ftrl.fromJsonString(json)
    println(s"ftrl fetched from hdfs: $key")
    return ftrl
  }

  def getModel(version: Int, startFresh: Boolean, typename: String, size: Int): Ftrl = {
    val ftrlNew = new Ftrl(size)
    val ftrlRedis = RedisUtil.redisToFtrlWithType(typename, version, size)
    val ftrl = if (ftrlRedis != null && !startFresh) {
      println("ftrl fetched from redis")
      ftrlRedis
    } else {
      println("new ftrl")
      ftrlNew
    }
    return ftrl
  }

  def saveModelToHDFS(key: String, ctx: SparkSession, ftrl: Ftrl): Unit = {
    val fs = FileSystem.get(ctx.sparkContext.hadoopConfiguration)
    val output = fs.create(new Path(key))
    val os = new BufferedOutputStream(output)
    os.write(ftrl.toJsonString.getBytes("UTF-8"))
    os.flush()
    os.close()
    println(s"save model to hdfs: $key")
  }

  def saveToProtoToHDFS(key: String, ctx: SparkSession, ftrl: Ftrl): Unit = {
    val proto = new FtrlProto(
      alpha = ftrl.alpha,
      beta = ftrl.beta,
      l1 = ftrl.L1,
      l2 = ftrl.L2,
      n = ftrl.nDict.toMap,
      z = ftrl.zDict.toMap,
      w = ftrl.wDict.toMap
    )
    val fs = FileSystem.get(ctx.sparkContext.hadoopConfiguration)
    val os = new BufferedOutputStream(fs.create(new Path(key)))
    proto.writeTo(os)
    os.close()
    println(s"save model proto to hdfs: $key")
  }

  def saveProtoToLocal(path: String, ftrl: Ftrl): Unit = {
    val proto = new FtrlProto(
      alpha = ftrl.alpha,
      beta = ftrl.beta,
      l1 = ftrl.L1,
      l2 = ftrl.L2,
      n = ftrl.nDict.toMap,
      z = ftrl.zDict.toMap,
      w = ftrl.wDict.toMap
    )
    Utils.saveProtoToFile(proto, path)
  }

  def getModelFromProtoOnHDFS(startFresh: Boolean, key: String, ctx: SparkSession): Ftrl = {
    val ftrl = new Ftrl(1)
    if (startFresh) {
      println("new ftrl")
      return ftrl
    }
    val fs = FileSystem.get(ctx.sparkContext.hadoopConfiguration)
    val proto = new FtrlProto().mergeFrom(CodedInputStream.newInstance(fs.open(new Path(key))))
    ftrl.alpha = proto.alpha
    ftrl.beta = proto.beta
    ftrl.L1 = proto.l1
    ftrl.L2 = proto.l2
    ftrl.nDict = mutable.Map() ++ proto.n.toSeq
    ftrl.zDict = mutable.Map() ++ proto.z.toSeq
    ftrl.wDict = mutable.Map() ++ proto.w.toSeq
    println(s"ftrl proto fetched from hdfs: $key")
    return ftrl
  }

  def getModelProtoFromLocal(startFresh: Boolean, path: String): Ftrl = {
    val ftrl = new Ftrl(1)
    if (startFresh) {
      println("new ftrl")
      return ftrl
    }
    val proto = new FtrlProto().mergeFrom(CodedInputStream.newInstance(new FileInputStream(path)))
    ftrl.alpha = proto.alpha
    ftrl.beta = proto.beta
    ftrl.L1 = proto.l1
    ftrl.L2 = proto.l2
    ftrl.nDict = mutable.Map() ++ proto.n.toSeq
    ftrl.zDict = mutable.Map() ++ proto.z.toSeq
    ftrl.wDict = mutable.Map() ++ proto.w.toSeq
    println(s"ftrl proto fetched from local: $path")
    return ftrl
  }


  def saveLrPbPack(ftrl: Ftrl, path: String, parser: String, name: String): Unit = {
    val lr = LRModel(
      parser = parser,
      featureNum = ftrl.w.length,
      weights = ftrl.w.zipWithIndex.toMap.map(x => (x._2, x._1))
    )
    val ir = IRModel(
    )
    val pack = Pack(
      name = name,
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(ftrl.dict),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/ctr-portrait9-qtt-list.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    Utils.saveProtoToFile(pack, path)
  }


  def saveLrPbPackWithDict(ftrl: Ftrl, path: String, parser: String, name: String): Unit = {
    val lr = LRModel(
      parser = parser,
      featureNum = ftrl.wDict.size,
      weights = ftrl.wDict.toMap
    )
    val ir = IRModel(
    )
    val pack = Pack(
      name = name,
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(ftrl.dict),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/ctr-portrait9-qtt-list.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    Utils.saveProtoToFile(pack, path)
  }

  def serializeLrToLocal(ftrl: Ftrl, path: String): Unit = {
    val serializable = new FtrlSerializable()
    serializable.alpha = ftrl.alpha
    serializable.beta = ftrl.beta
    serializable.L1 = ftrl.L1
    serializable.L2 = ftrl.L2
    serializable.nDict = ftrl.nDict.toMap
    serializable.zDict = ftrl.zDict.toMap
    serializable.wDict = ftrl.wDict.toMap

    val outFile = new File(path)
    outFile.getParentFile.mkdirs()

    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(serializable)
    oos.close()
    println(s"save to local: $path")
  }

  def deserializeFromLocal(startFresh: Boolean, path: String): Ftrl = {
    if (startFresh) {
      return new Ftrl(1)
    }
    println(s"fetching model from $path")
    val ois = new ObjectInputStream(new FileInputStream(path))
    val ftrlSerializable = ois.readObject.asInstanceOf[FtrlSerializable]
    ois.close()

    val ftrl = new Ftrl(1)
    ftrl.alpha = ftrlSerializable.alpha
    ftrl.beta = ftrlSerializable.beta
    ftrl.L1 = ftrlSerializable.L1
    ftrl.L2 = ftrlSerializable.L2
    ftrl.wDict = mutable.Map() ++ ftrlSerializable.wDict
    ftrl.zDict = mutable.Map() ++ ftrlSerializable.zDict
    ftrl.nDict = mutable.Map() ++ ftrlSerializable.nDict
    return ftrl
  }

}