package org.xuan

import com.alibaba.fastjson.JSONObject
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType

import scala.util.Random


/**
  * Created by zhangzhikuan on 16/7/2017.
  */

@Path("/")
class PostResource {

  import App.sqlContext.implicits._

  @Path("/post")
  @POST
  @Produces(Array(MediaType.APPLICATION_JSON): _*)
  def post(req: String): String = {
    val df = (1 to Random.nextInt(100)).toDF("id")
    val obj = new JSONObject()
    obj.put("name", df.dropDuplicates("id").count())
    obj.toJSONString()
  }
}
