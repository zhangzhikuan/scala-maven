package com.xuan

import com.alibaba.fastjson.{JSONArray, JSONObject}
import scala.collection.JavaConverters._
object JSONUtils {
  //黑名单
  val blackWord = List(",", ";", "{", "}", "(", ")","""\n""","""\t""","""=""")

  /**
    * 扁平化ｊｓｏｎObject
    *
    * @param obj       对象
    * @param basePath  基础路径
    * @param out       输出
    * @param isFlatMap 是否加上
    */
  def flatJsonObject(obj: JSONObject,
                     basePath: Option[String],
                     out: JSONObject  ,
                     isFlatMap: Boolean = true): Unit = {
    //基础 root key
    val bp = basePath match {
      case None => ""
      case Some(p) => if (isFlatMap) s"${p}." else ""
    }
    //遍历所有的key
    val keys = obj.keySet().asScala
    keys.foreach {
      key =>
        val trimKey = key.replaceAll(" ", "")

        //如果key有非法字符，则直接抛出异常
        blackWord.foreach(b => if (trimKey.contains(b)) throw new RuntimeException(s"[$trimKey]存在非法字符[${b}]"))

        obj.get(key) match {
          case array: JSONArray => //目前不解析数组
            out.put(s"$bp$trimKey", array.toJSONString)
          case obj: JSONObject => //解析对象，嵌套对象也解析
            flatJsonObject(obj, Some(s"$bp$key"), out, isFlatMap)
          case v =>
            if (v != null)
              out.put(s"$bp$trimKey", v.toString) //单个值也需要解析
            else
              out.put(s"$bp$trimKey", null) //单个值也需要解析
        }
    }
  }
}
