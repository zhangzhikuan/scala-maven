package org.xuan

import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * @author ${user.name}
  */
object App {


  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("accumulator_study")
    val sc = new SparkContext(conf)

    //此处两种定义方式，默认 LongAccumulatorParam DoubleAccumulatorParam 隐式转换
    //如果想自定义累加器，那么只要实现AccumulatorParam接口即可
    //val ac = sc.accumulator(0L)
    val ac = sc.accumulator(0L)(LongAccumulatorParam)
    //生成测试RDD
    val seq = Seq(1, 2, 3, 4, 5)
    val sum = sc.parallelize(seq).map {
      v =>
        ac += 1
        v * 10
    }.sum()
    //RDD的每个值都乘10，然后求和
    println(s"last sum:${sum}")
    println(s"last acculator:${ac.value}")

  }

}
