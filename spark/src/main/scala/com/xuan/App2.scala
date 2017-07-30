package com.xuan

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaApusUtils, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhangzhikuan on 17-7-22.
  */
object App2 {
  def main(args: Array[String]): Unit = {
    val appName = "spark_Streaming_topic"
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName(appName)

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    //val df = List(1,2,3,4,5).map(x=>(x,x)).toDF("id","age")

    //df.write.parquet("/tmp/dff")
    val df = sqlContext.read.parquet("/tmp/dff")
    df.persist()
    df.select("id").collect()

    df.printSchema()
    sc.stop
  }
}
