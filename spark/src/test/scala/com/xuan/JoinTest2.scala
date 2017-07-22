package com.xuan

import java.util.concurrent.Semaphore

/**
  * Created by zhangzhikuan on 17-7-13.
  */
object JoinTest2 {
  def main(args: Array[String]): Unit = {
    val sp= new Semaphore(10)
    sp.availablePermits()

  }
}
