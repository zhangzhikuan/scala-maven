package org.xuan

import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * @author ${user.name}
  */
case class Item(id: Long, company: String, app: String, download: Long)

case class ItemAnylize(id: Long, download: Long, ids: Long) {
}

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

    val zero = (id_v: (Long, (Long, Long))) => Map[Long, (Long, Long)](id_v._1 -> (id_v._2._1, id_v._2._2))

    val mergeValue = (map: Map[Long, (Long, Long)], id_v: (Long, (Long, Long))) => {
      val d = map.getOrElse(id_v._1, (0L, 0L))
      val dd = (d._1 + id_v._2._1, d._2 + id_v._2._2)
      map.update(id_v._1, dd)
      map
    }
    val mergeCombiners = (map1: Map[Long, (Long, Long)], map2: Map[Long, (Long, Long)]) => {
      map2.foldLeft(map1) { (mmap, id_v) =>
        mergeValue(mmap, id_v)
      }
      map1
    }

    val rdd = sc.parallelize(List(1, 2, 3, 4, 5)).map(x => Item(x.toLong, "tx", "wx", 1L)).map {
      case info =>
        (s"${info.company}_${info.app}_${info.id}", (info.id, (info.download, 1L)))
    }.combineByKey[Map[Long, (Long, Long)]](zero, mergeValue, mergeCombiners)


  }


}
