package org.xuan

import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhangzhikuan on 27/6/2017.
  */
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
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
