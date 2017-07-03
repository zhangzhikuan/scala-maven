package org.xuan

import java.io.{File, FileWriter}
import java.sql.Timestamp

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.concurrent.duration._
import org.apache.spark.sql.streaming.ProcessingTime

/**
  * Created by zhangzhikuan on 27/6/2017.
  */
case class GB(timestamp: Timestamp, word: String)

object BU {
  def main(args: Array[String]): Unit = {


    //此处两种定义方式，默认 LongAccumulatorParam DoubleAccumulatorParam 隐式转换
    //如果想自定义累加器，那么只要实现AccumulatorParam接口即可
    //val ac = sc.accumulator(0L)

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Word Count")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    // Split the lines into words, retaining timestamps
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")


    val windowedCounts =
      words.groupBy(
        //        window($"timestamp", "10 seconds", "5 seconds"), $"word"
        window($"timestamp", "10 seconds")
      ).count()

    val query = windowedCounts.writeStream
      .outputMode("update")
      .format("console")
      .trigger(ProcessingTime(5.seconds))
      .start()

    query.awaitTermination()

  }
}
