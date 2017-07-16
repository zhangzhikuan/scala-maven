package org.xuan

import com.sun.jersey.spi.container.servlet.ServletContainer
import org.apache.spark.AccumulatorParam.LongAccumulatorParam
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

import scala.collection.mutable.Map


object App {

  private val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.setAppName("accumulator_study")
  private val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    try {
      //throw new RuntimeException
      startServer()
    } catch {
      case ex: Exception => sc.stop()
    }
  }

  private def startServer(): Unit = {
    val server = new Server(8080)
    val context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS)
    context.setContextPath("/")
    server.setHandler(context)
    val sh = new ServletHolder(classOf[ServletContainer])
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig")
    sh.setInitParameter("com.sun.jersey.config.property.packages", "org.xuan")
    context.addServlet(sh, "/*")
    server.start()
  }


}
