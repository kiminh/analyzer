package com.cpc.spark.ml.train

import scala.util.control.Breaks._
import scala.io.Source
import org.apache.spark.{AccumulableParam, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo._

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel

//////////////////////////////////////////////////////////////////////////
object CommonFunc {

  class SparseVector(val indices: Array[Int], val values: Array[Double]) extends Serializable {
    require(indices.length == values.length)

    def map(f: Double => Double): SparseVector = {
      new SparseVector(indices, values.map(x => f(x)).toArray)
    }

    def *(v: Vector): Double = {
      var dotSum = 0.0
      var i = 0
      while (i < indices.length) {
        dotSum += values(i) * v.data(indices(i))
        i += 1
      }
      dotSum
    }
  }

  case class WeightedDataPoint(x: SparseVector, y: Int, z: Double) extends Serializable

  def parseWeightedPoint(line: String, sign_map_b: Map[String, Array[String]], click_weight: Int, noclick_weight: Double): WeightedDataPoint = {
    val fields = line.split("\t")
    var filter = false
    if (fields.length <= 1)
      return WeightedDataPoint(new SparseVector(Array[Int](1), Array[Double](1)), 2, 0.0)

    var y = fields(1).toInt
    var weight = 1.0
    if (y == 1) {
      weight *= click_weight
    }
    else if (y == 0) {
      y = -1
      if (scala.math.random > noclick_weight)
        return WeightedDataPoint(new SparseVector(Array[Int](1), Array[Double](1)), 2, 0.0)
    }
    else
      return WeightedDataPoint(new SparseVector(Array[Int](1), Array[Double](1)), 2, 0.0)

    var indices = new ArrayBuffer[Int]()
    var values = new ArrayBuffer[Double]()
    fields.filter(_.length() > 5).foreach(feature => {
      if (sign_map_b.contains(feature)) {
        indices += (sign_map_b(feature)(0).toInt)
        values += (sign_map_b(feature)(1).toDouble)
      }
    })
    WeightedDataPoint(new SparseVector(indices.toArray, values.toArray), y, weight)
  }

  class Vector(val data: Array[Double]) extends Serializable {

    def length: Int = {
      data.length
    }

    def *(scale: Double): Vector = {
      var vectorSum = new Vector(Array.tabulate(data.length)(_ => 0.0))
      var i = 0
      while (i < data.length) {
        vectorSum.data(i) = data(i) * scale
        i += 1
      }
      vectorSum
    }

    def *(v: Vector): Double = {
      var dotSum = 0.0
      if (data.length != v.length) {
        throw new IllegalArgumentException
        System.exit(1)
      }
      var i = 0
      while (i < data.length) {
        dotSum += data(i) * v.data(i)
        i += 1
      }
      dotSum
    }

    def *=(scale: Double): Vector = {
      var i = 0
      while (i < data.length) {
        data(i) = data(i) * scale
        i += 1
      }
      this
    }

    def dot(v: Vector): Double = {
      var dotSum = 0.0
      if (data.length != v.length) {
        throw new IllegalArgumentException
        System.exit(1)
      }
      var i = 0
      while (i < data.length) {
        dotSum += data(i) * v.data(i)
        i += 1
      }
      dotSum
    }

    def +(v: Vector): Vector = {
      if (data.length != v.length) {
        System.err.println("Length does NOT match: " +
          data.length + "(Left length) !=  " + v.length + "(Right length) !")
        throw new IllegalArgumentException()
      }

      var vectorSum = new Vector(Array.tabulate(data.length)(_ => 0.0))
      var i = 0
      while (i < data.length) {
        vectorSum.data(i) = data(i) + v.data(i)
        i += 1
      }
      vectorSum
    }

    def +=(v: Vector): Vector = {
      if (data.length != v.length) {
        System.err.println("Length does NOT match: " +
          data.length + "(Left length) !=  " + v.length + "(Right length) !")
        throw new IllegalArgumentException()
      }
      var i = 0
      while (i < data.length) {
        data(i) = data(i) + v.data(i)
        i += 1
      }
      this
    }

    def -(v: Vector): Vector = {
      if (data.length != v.length) {
        System.err.println("Length does NOT match: " +
          data.length + "(Left length) !=  " + v.length + "(Right length) !")
        throw new IllegalArgumentException()
      }
      var vectorSum = new Vector(Array.tabulate(data.length)(_ => 0.0))
      var i = 0
      while (i < data.length) {
        vectorSum.data(i) = data(i) - v.data(i)
        i += 1
      }
      vectorSum
    }

    def -=(v: Vector): Vector = {
      if (data.length != v.length) {
        System.err.println("Length does NOT match: " +
          data.length + "(Left length) !=  " + v.length + "(Right length) !")
        throw new IllegalArgumentException()
      }
      var i = 0
      while (i < data.length) {
        data(i) = data(i) - v.data(i)
        i += 1
      }
      this
    }

    def norm1(): Double = {
      var normSum = 0.0
      var i = 0
      while (i < data.length) {
        normSum += Math.abs(data(i))
        i += 1
      }
      normSum
    }

    def norm2(): Double = {
      var normSum = 0.0
      var i = 0
      while (i < data.length) {
        normSum += data(i) * data(i)
        i += 1
      }
      Math.sqrt(normSum)
    }

    override def toString(): String = {
      var str = "["
      var i = 0
      while (i < data.length) {
        str = str + data(i) + ","
        i += 1
      }
      str = str.dropRight(1) + "]"
      str
    }
  }

  object Vector {
    def zeros(size: Int): Vector = {
      new Vector(Array.tabulate(size)(_ => 0.0))
    }
  }

  implicit object VectorAP extends AccumulableParam[Vector, SparseVector] {
    def zero(v: Vector) = new Vector(new Array(v.data.size))

    def addInPlace(v1: Vector, v2: Vector): Vector = {
      var i = 0
      while (i < v1.data.size) {
        v1.data(i) += v2.data(i)
        i += 1
      }
      v1
    }

    def addAccumulator(v1: Vector, v2: SparseVector): Vector = {
      var i = 0
      while (i < v2.indices.length) {
        v1.data(v2.indices(i)) += v2.values(i)
        i += 1
      }
      v1
    }
  }

  case class IterationInfo(s: Vector, y: Vector, rho: Double)

}

class EnvClass {
  var spark_home = ""
  var hdfs_home = ""
  var input_path = ""
  var model_file = ""
  var done_file = ""
  var sample_weight = 0
  var fe_thr = 0
  var init_weight = ""

  val patition_num = 50
  var click_weight = 0
  var noclick_weight = 0.0
  var output_arr = new ArrayBuffer[String]()

  /////////////////////////////////////////////////////////////////
  def init(args: Array[String]): SparkContext = {
    if (args.length < 7) {
      val err_info = "Usage: <SparkHome:String> " +
        "<HDFSHome:String> " +
        "<InputPath:String> " +
        "<ModelPath:String> " +
        "<jobDoneFile:String> " +
        "<SampleWeight:String> " +
        "<FeatureThrehold:String> " +
        "[<initWeightFile:String>]"
      println(err_info)
      output_arr += (err_info)
      System.exit(1);
    }

    spark_home = args(0)
    hdfs_home = args(1)
    input_path = args(2)
    model_file = args(3)
    done_file = args(4)
    sample_weight = args(5).toInt
    fe_thr = args(6).toInt
    if (args.length > 7)
      init_weight = args(7)

    output_arr += ("input_path" + "\t" + input_path)
    output_arr += ("hdfs_home" + "\t" + hdfs_home)
    output_arr += ("model_file" + "\t" + model_file)
    output_arr += ("done_file" + "\t" + done_file)
    output_arr += ("sample_weight" + "\t" + sample_weight)
    output_arr += ("fe_thr" + "\t" + fe_thr)
    output_arr += ("init_weight" + "\t" + init_weight)

    val conf = new SparkConf()
    //conf.setAppName("ModelTraining-OWLQN")
    conf.setSparkHome(spark_home)
    //conf.setJars(List(jarFile))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "LRRegistrator")
    conf.set("spark.storage.memoryFraction", "0.7")
    conf.set("spark.akka.frameSize", "400")
    conf.set("spark.speculation", "true")
    conf.set("spark.storage.blockManagerHeartBeatMs", "300000")
    conf.set("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
    conf.set("spark.core.connection.auth.wait.timeout", "100")
    conf.set("spark.akka.frameSize", "100")
    //conf.set("spark.executor.memory", "64g")
    //conf.set("spark.cores.max", "64")

    new SparkContext(conf)
  }

  def write(sc: SparkContext, owlqn: OWLQN, feat_sign: FeatSign) {
    var i = 0
    while (i < feat_sign.sign_array.length) {
      feat_sign.sign_array(i) = feat_sign.sign_array(i) + "\t" + owlqn.x_new.data(i) * feat_sign.sign_map(feat_sign.sign_array(i))(1).toDouble
      i += 1
    }

    val model_rdd = sc.makeRDD(feat_sign.sign_array, 1).filter(x => x.split("\t")(1) != "0.0")
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs_home), hadoopConf)

    //val model_rdd = sc.makeRDD(sign_array, 1)
    //val out_path = model_file + "_" + (scala.math.random*100).toInt
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(model_file), true)
    } catch {
      case _: Throwable => {}
    }
    println("write data to: " + model_file)
    output_arr += ("write data to: " + model_file)
    model_rdd.saveAsTextFile(model_file)

    //var out = new java.io.FileWriter("./out.txt")
    //for(i <- 0 until (sign_array.length - 1)){
    //    out.write(sign_array(i) + "\n")
    //}
    //out.close()
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(done_file), true)
    } catch {
      case _: Throwable => {}
    }
    var done_rdd = sc.makeRDD(output_arr, 1)
    println(output_arr)
    done_rdd.saveAsTextFile(done_file)
  }
}

class FeatSign {
  var sign_map: Map[String, Array[String]] = Map()
  var sign_array = new ArrayBuffer[String]
  var dim = 0
}

class TrainData(sc: SparkContext, env_class: EnvClass, feat_sign: FeatSign) {

  class NotSerializable(val n: Int)

  var source_rdd: RDD[String] = sc.makeRDD(Array(""))
  var train_rdd: RDD[CommonFunc.WeightedDataPoint] = sc.makeRDD(Array(CommonFunc.WeightedDataPoint(new CommonFunc.SparseVector(Array[Int](1), Array[Double](1)), 2, 0.0)))

  // 正负样本统计，特征CTR统计
  def sample_stat() {
    var positive = sc.accumulator(1)
    var negative = sc.accumulator(1)
    for (line <- source_rdd) {
      if (line.split("\t")(1).toInt == 1)
        positive += 1
      else
        negative += 1
    }
    println("positive = " + positive.value)
    println("negative = " + negative.value)
    env_class.output_arr += ("positive = " + positive.value)
    env_class.output_arr += ("negative = " + negative.value)

    env_class.click_weight = env_class.sample_weight
    env_class.noclick_weight = (positive.value * 1.0 / (negative.value + positive.value)) * env_class.sample_weight

    println("click_weight = " + env_class.sample_weight)
    println("noclick_weight = " + env_class.noclick_weight)
    env_class.output_arr += ("click_weight = " + env_class.sample_weight)
    env_class.output_arr += ("noclick_weight = " + env_class.noclick_weight)
  }

  def get_feat_sign() {
    val sign_rdd = source_rdd.flatMap { line =>
      val fields = line.split("\t")
      var click = 0
      if (fields(1).toInt == 1)
        click = 1

      var sign_stat = new ArrayBuffer[(String, (Int, Int))]()
      fields.filter(_.length() > 5).foreach(feature => {
        sign_stat += ((feature, (click, 1)))
      })
      sign_stat
    }.reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }
    //println("sign_rdd.count = " + sign_rdd.count)
    //env_class.output_arr += ("sign_rdd.count = " + sign_rdd.count)
    //println(sign_rdd.collect())

    // sign 标号
    feat_sign.sign_array = new ArrayBuffer[String]()
    var idx = 0
    val obj_thr = new NotSerializable(env_class.fe_thr)
    val v_thr = obj_thr.n
    sign_rdd.filter(_._2._2 > v_thr).collect().foreach(x => {
      val sign = x._1
      val ctr = x._2._1 * 1.0 / x._2._2
      var value = new Array[String](2)
      value(0) = idx.toString
      value(1) = ctr.toString
      feat_sign.sign_map += (sign -> value)
      feat_sign.sign_array += (sign)
      idx += 1
    })
  }

  def get_trainset() {
    var sign_map_b = sc.broadcast(feat_sign.sign_map)
    var click_weight_b = sc.broadcast(env_class.click_weight)
    var noclick_weight_b = sc.broadcast(env_class.noclick_weight)
    train_rdd = source_rdd.map(CommonFunc.parseWeightedPoint(_, sign_map_b.value, click_weight_b.value, noclick_weight_b.value)).filter(_.y != 2).repartition(env_class.patition_num).persist(StorageLevel.MEMORY_ONLY)

    //println("trainset.count = " + train_rdd.count)
    //env_class.output_arr += ("trainset.count = " + train_rdd.count)
  }

  def get_data() {
    source_rdd = sc.textFile(env_class.input_path, env_class.patition_num)
    sample_stat()
    get_feat_sign()
    get_trainset()
    source_rdd = sc.makeRDD(Array("")) // 释放资源
  }
}

class OWLQN(sc: SparkContext, env_class: EnvClass, feat_sign: FeatSign, train_data: TrainData) {
  val C = 1e-3 // regularization trade-off
  val m_eps = 1e-6 // stop condition for gradient 2-norm   1e-8
  val m_c1 = 1e-4 // Armijo condition
  val m_c2 = 0.9 // Curvature condition (not used)
  val ITERATIONS = 100 // max iterations
  val M = 10
  val score_th = 30

  /////////////////////////////////
  var dim_num = 0

  var f_val_old = 0.0
  var f_val = 0.0

  var x = CommonFunc.Vector.zeros(dim_num)
  var x_new = CommonFunc.Vector.zeros(dim_num)

  var gradient = CommonFunc.Vector.zeros(dim_num)
  var gradient_new = CommonFunc.Vector.zeros(dim_num)

  var direction = CommonFunc.Vector.zeros(dim_num)
  var steepestDescDir = CommonFunc.Vector.zeros(dim_num)

  var sList = Array[CommonFunc.Vector]()
  var yList = Array[CommonFunc.Vector]()

  var rhoList = Array[Double]()
  var alphas = Array.tabulate(M)(_ => 0.0)

  ////////////////////////////////////////////////////////
  def train_init() {
    dim_num = feat_sign.sign_array.length

    //val arrayZero = Array.tabulate(dim_num)(_ => 0.0)
    println("dim_num = " + dim_num)
    env_class.output_arr += ("dim_num = " + dim_num)
    // initialization stage
    // 初始点
    x = CommonFunc.Vector.zeros(dim_num)
    if (env_class.init_weight != "") {
      var init_idx = 0
      for (line <- Source.fromFile(env_class.init_weight).getLines) {
        x.data(init_idx) = line.toDouble
        init_idx += 1
      }
    }

    x_new = CommonFunc.Vector.zeros(dim_num)

    gradient = CommonFunc.Vector.zeros(dim_num)
    gradient_new = CommonFunc.Vector.zeros(dim_num)

    direction = CommonFunc.Vector.zeros(dim_num)
    steepestDescDir = CommonFunc.Vector.zeros(dim_num)

    sList = Array[CommonFunc.Vector]()
    yList = Array[CommonFunc.Vector]()

    rhoList = Array[Double]()
    alphas = Array.tabulate(M)(_ => 0.0)
  }

  def scan_trainset(x: CommonFunc.Vector) {
    var gAcc = sc.accumulable(CommonFunc.Vector.zeros(dim_num))(CommonFunc.VectorAP)
    var fAcc = sc.accumulator(1.0)
    var xb = sc.broadcast(x) // weight
    var score_th_b = sc.broadcast(score_th) // weight
    for (p <- train_data.train_rdd) {
      var score = 0.0
      var i = 0
      while (i < p.x.indices.length) {
        score += p.x.values(i) * xb.value.data(p.x.indices(i))
        i += 1
      }

      if (p.y == -1)
        score = -1 * score
      var insLoss = 0.0
      var insProb = 0.0
      if (score < -1 * score_th_b.value) // f_val过小
      {
        insLoss = -1 * score;
        insProb = 0;
      }
      else if (score > score_th_b.value) // f_val过大
      {
        insLoss = 0;
        insProb = 1;
      }

      else {
        var temp = 1.0 + Math.exp(-1 * score);
        insLoss = Math.log(temp);
        insProb = 1.0 / temp;
      }
      fAcc += insLoss * p.z

      var mult = 1.0 - insProb
      if (p.y == 1)
        mult *= -1
      gAcc += p.x.map(v => v * mult * p.z)
    }
    println("fAcc = " + fAcc.value)
    env_class.output_arr += ("fAcc = " + fAcc.value)

    var reg = 0.0
    for (i <- 0 until dim_num - 1)
      reg += Math.abs(x.data(i)) * C
    f_val = fAcc.value + reg
    gradient_new = new CommonFunc.Vector(gAcc.value.data)
  }

  def psudo_gradient() {
    println("---MakeSteepestDescDir---")
    env_class.output_arr += ("---MakeSteepestDescDir---")
    for (i <- 0 until dim_num - 1) {
      if (x.data(i) < 0)
        direction.data(i) = -gradient.data(i) + C;
      else if (x.data(i) > 0)
        direction.data(i) = -gradient.data(i) - C;
      else {
        if (gradient.data(i) < -C)
          direction.data(i) = -gradient.data(i) - C;
        else if (gradient.data(i) > C)
          direction.data(i) = -gradient.data(i) + C;
        else
          direction.data(i) = 0;
      }
    }

    //steepestDescDir = direction
    steepestDescDir = new CommonFunc.Vector(direction.data)
  }

  def too_loop() {
    // MapDirByInverseHessian
    println("---MapDirByInverseHessian---")
    println("sList.size:" + sList.size)
    println("yList.size:" + yList.size)
    println("rhoList.size:" + rhoList.size)
    println("alphas.size:" + alphas.size)
    env_class.output_arr += ("---MapDirByInverseHessian---")
    env_class.output_arr += ("sList.size:" + sList.size)
    env_class.output_arr += ("yList.size:" + yList.size)
    env_class.output_arr += ("rhoList.size:" + rhoList.size)
    env_class.output_arr += ("alphas.size:" + alphas.size)

    val count = sList.size
    if (count != 0) {
      for (i <- (count - 1) to 0 by -1) {
        alphas(i) = -1 * (sList(i) * direction) / rhoList(i);
        direction += yList(i) * alphas(i);
      }

      val lastY = yList(count - 1)
      val yDotY = lastY * lastY
      val scalar = rhoList(count - 1) / yDotY; // 求H0
      direction *= scalar

      for (i <- 0 to (count - 1)) {
        var beta = yList(i) * direction / rhoList(i);
        direction += sList(i) * (-alphas(i) - beta);
      }
    }

    // FixDirSigns
    for (i <- 0 until dim_num - 1) {
      if (direction.data(i) * steepestDescDir.data(i) <= 0)
        direction.data(i) = 0;
    }
  }

  def BackTrackingLineSearch(it: Int) {
    // BackTrackingLineSearch -----------------------------------------------------
    println("---BackTrackingLineSearch---")
    println("f_val = " + f_val)
    env_class.output_arr += ("---BackTrackingLineSearch---")
    env_class.output_arr += ("f_val = " + f_val)

    var origDirDeriv = 0.0
    for (i <- 0 until dim_num - 1) {
      if (direction.data(i) != 0) {
        if (x.data(i) < 0)
          origDirDeriv += direction.data(i) * (gradient.data(i) - C)
        else if (x.data(i) > 0)
          origDirDeriv += direction.data(i) * (gradient.data(i) + C);
        else if (direction.data(i) < 0)
          origDirDeriv += direction.data(i) * (gradient.data(i) - C);
        else if (direction.data(i) > 0)
          origDirDeriv += direction.data(i) * (gradient.data(i) + C);
      }
    }

    if (origDirDeriv >= 0) {
      println("L-BFGS chose a non-descent direction: check your gradient!")
      env_class.output_arr += ("L-BFGS chose a non-descent direction: check your gradient!")
      System.exit(1);
    }

    ////---
    var alpha = 1.0
    var backoff = 0.2
    if (it == 1) {
      var normDir = Math.sqrt(direction * direction)
      alpha = (1 / normDir)
      backoff = 0.1
    }

    var f_val_old = f_val

    println("Backtracing loop")
    env_class.output_arr += ("Backtracing loop")
    breakable {
      while (true) {
        //GetNextPoint(alpha);
        //addMultInto(x_new, x, dir, alpha);
        x_new = x + (direction * alpha)
        // 搜索下一个节点 不能cross 象限
        for (i <- 0 until dim_num - 1) {
          if (x.data(i) * x_new.data(i) < 0.0)
            x_new.data(i) = 0.0
        }

        //EvalL1()
        //Eval(x_new, newGrad)
        scan_trainset(x_new)

        if (f_val <= f_val_old + m_c1 * origDirDeriv * alpha) {
          println("Armijo condition satisfied!")
          env_class.output_arr += ("Armijo condition satisfied!")
          break
        }
        alpha *= backoff
        if (alpha < 1e-20) {
          println("alpha is less than 1e-20! break...")
          env_class.output_arr += ("alpha is less than 1e-20! break...")
          break
        }
      } //end of BackTrackingLineSearch
    }

    if (Math.abs(f_val - f_val_old) / f_val_old < 0.00001) {
      println("new f_val: " + f_val + " , tiny modification!")
      env_class.output_arr += ("new f_val: " + f_val + " , tiny modification!")
      break
    }
  }

  def shift() {
    // Shift
    println("---Shift to the new weight---")
    env_class.output_arr += ("---Shift to the new weight---")

    var nextS = x_new - x
    var nextY = gradient_new - gradient
    var rho = nextS * nextY

    sList = sList.:+(nextS)
    yList = yList.:+(nextY)
    rhoList = rhoList.:+(rho)
    if (sList.length > M) {
      sList = sList.dropRight(1)
      yList = yList.dropRight(1)
      rhoList = rhoList.dropRight(1)
    }
  }

  def train() {
    train_init()

    // 第一次扫描数据
    scan_trainset(x)
    gradient = new CommonFunc.Vector(gradient_new.data)

    // iteration stage
    // 1. take the longest step with loss decreasing sufficiently
    // 2. calculate the new point & direction
    breakable {
      for (it <- 1 to ITERATIONS) {
        println("Iteration : " + it)
        env_class.output_arr += ("Iteration : " + it)
        psudo_gradient()
        too_loop()
        BackTrackingLineSearch(it)
        shift()
        ///////////////////////////////////////////////////////
        x = new CommonFunc.Vector(x_new.data)
        gradient = new CommonFunc.Vector(gradient_new.data)
      } //end of iteration
    } //end of breakable
  }
}

class LRRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[CommonFunc.SparseVector])
    kryo.register(classOf[CommonFunc.Vector])
    kryo.register(classOf[CommonFunc.WeightedDataPoint])
  }
}

object LogisticRegression {
  def main(args: Array[String]): Unit = {
    var env_class = new EnvClass()
    var feat_sign = new FeatSign()

    var sc = env_class.init(args)

    var train_data = new TrainData(sc, env_class, feat_sign)
    train_data.get_data()

    var owlqn = new OWLQN(sc, env_class, feat_sign, train_data)
    owlqn.train()

    env_class.write(sc, owlqn, feat_sign)
    sc.stop()
  }
}
