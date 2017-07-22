package com.xuan

import javax.ws.rs.{POST, Path, Produces}
import javax.ws.rs.core.MediaType

import com.alibaba.fastjson.JSONObject

import scala.util.Random


/**
  * Created by zhangzhikuan on 16/7/2017.
  */

@Path("/")
class PostResource {

  @Path("/post")
  @POST
  @Produces(Array(MediaType.APPLICATION_JSON): _*)
  def post(req: String): String = {
    //val df = (1 to Random.nextInt(100)).toDF("id")
    val obj = new JSONObject()
   // obj.put("name", df.dropDuplicates("id").count())
    obj.toJSONString()
  }
}
