package com.liz.utils

import com.alibaba.fastjson.{JSON, JSONObject}


/**
  * Created by liz
  */
object JsonParse {
  def parse(str:String):(String  , Any) =  {
   // println(str)
    val json: JSONObject = JSON.parseObject(str)
    val table = if(json.getString("table") != null) json.getString("table") else ""
    val type_ = if(json.getString("type") != null) json.getString("type") else "insert"
    val data = json.getJSONObject("data")
    //此处判别表
    val bean:(String , Any) = table match{
      case _ =>
        ("#A" , "#A")
    }
    bean
  }
  def parseJsonStr(jsonStr:String):Any ={
    val jsonObj: JSONObject = JSON.parseObject(jsonStr)
    jsonObj.getString("")
  }
}
