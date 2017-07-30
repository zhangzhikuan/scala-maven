package com.alibaba.fastjson

import java.lang.reflect.Type


/**
  * Created by zhangzhikuan on 22/7/2017.
  */
object Appe {
  def main(args: Array[String]): Unit = {
    val json =new JSONArray()
    json.componentType=new Type(){

    }
     println(json.componentType)
  }
}
