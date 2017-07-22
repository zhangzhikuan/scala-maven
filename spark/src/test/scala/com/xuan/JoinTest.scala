package com.xuan

import java.util.concurrent.ArrayBlockingQueue
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}
import org.glassfish.jersey.servlet.ServletContainer
/**
  * Created by zhangzhikuan on 17-7-13.
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("accumulator_study")
    val sc = new SparkContext(conf)
    val sq = new SQLContext(sc)
    import sq.implicits._

    //val seq=Seq((1,"kuan"),(3,"kuan"),(3,"kuan"),(3,"wei"),(4,"wei"))
    // val seq2=Seq((2,"kuan",10),(2,"kuan",10),(2,"kuan",11),(2,"kuan",12),(2,"wei",10),(2,"wei",12))
    //val left =sc.makeRDD(seq).toDF("id","name").select("id","name").filter($"id" === 3).dropDuplicates("name")
    //val right=sc.makeRDD(seq2).toDF("id","name","hour").select("id","name","hour").filter($"id"===2).dropDuplicates("name","hour")

    val poll = new ArrayBlockingQueue[Int](5)

    new Thread() {
      override def run(): Unit = {
        while (true) {
          poll.put(1)
          Thread.sleep(1000*10)
        }
      }

    }.start()

    while (poll.take() != null) {
      //val xalEventBaseDir = "/home/zhangzhikuan/Downloads/user/hive/warehouse/apuscommon.db/xal_basic/" //固定值
      //val path = xalEventBaseDir + "dt=2017-07-11" + "/" + "pn=vpn/*"
      //sq.read.parquet(path).show()
      val seq=Seq((1,"kuan"),(3,"kuan"),(3,"kuan"),(3,"wei"),(4,"wei")).map{
        case x=>
          x
      }.toDF("id","name").show()
    }

    Thread.sleep(1000 * 1000 * 1000)

  }
}
