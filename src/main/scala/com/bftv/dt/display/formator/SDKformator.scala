package com.bftv.dt.display.formator

import java.net.URLDecoder
import com.alibaba.fastjson.{JSON, JSONObject}
import com.bftv.dt.display.utils.IPParser
import org.slf4j.LoggerFactory
import scala.collection.mutable
import scala.util.parsing.json.JSONObject
/**
  * 解析日志message
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
class SDKFormator extends LogFormator {

  val logger = LoggerFactory.getLogger(this.getClass)

  val logRegex = """(\d+\.\d+\.\d+\.\d+).*?logger.php\?(.*?) HTTP.*""".r

  //query: enc=0&appkey=bftv&ltype=heart&log={%22uid%22:%22androidId1c7f471e675d6d61%22,%22imei%22:%22androidId1c7f471e675d6d61%22,%22userid%22:%22-%22,%22mac%22:%225c:ff:ff:82:c0:31%22,%22apptoken%22:%22282340ce12c5e10fa84171660a2054f8%22,%22ver%22:%223.1.0.397%22,%22mtype%22:%22BAOFENG_TV%20AML_T866%22,%22version%22:%223.0%22,%22androidid%22:%221c7f471e675d6d61%22,%22unet%22:%221%22,%22mos%22:%224.4.2%22,%22itime%22:%222019-03-05%2014:58:37%22,%22value%22:{%22sn%22:%22600000MWE00D169V2941_8AE6%22,%22plt_ver%22:%22V4.1.02%22,%22package_name%22:%22com.bftv.fui.launcher%22,%22pid%22:%22heart%22,%22lau_ver%22:%224.0.4.1664%22,%22plt%22:%22AML_T866H%22,%22softid%22:%2211161801%22,%22page_title%22:%22com.bftv.fui.launcher.views.activity.IndexRootActivity%22,%22ip%22:%2239.189.43.57%22},%22gid%22:%22dev%22,%22uuid%22:%22600000MWE00D169V2941_8AE6%22}
  // ip: 39.189.43.57
  /**
    * 解析整个message并返回一个json字符串
    * @param logStr 原始message
    * @param ipAreaIspCache 解析用的ipareaisp.txt
    * @param fields 需要解析出来的字段
    * @return json字符串
    */
  override def format(logStr: String, ipAreaIspCache: Array[(String, String, String, String, Long, Long)], fields: Array[String]): String = {
    var paramMap: mutable.Map[String, String] = mutable.Map[String, String]()
    var res :Map[String, String] = Map[String, String]()
    var jsonStr = ""
    var appkey = "-"
    try{
      val logRegex(ip, query) = logStr
      val iparea = IPParser.parse(ip,ipAreaIspCache)
      paramMap += "country" -> iparea._1
      paramMap += "province" -> iparea._2
      paramMap += "city" -> iparea._3
      paramMap += "isp" -> iparea._4
      val fieldsLogList = query.split("&").toList
      fieldsLogList.map(x => paramMap += x.split("=")(0) -> x.split("=")(1))
      if(!paramMap.getOrElse("enc", "0").equals("0")){
        paramMap("log") = decode(paramMap("log"))
        paramMap("ltype") = decode(paramMap("ltype"))
      }else{
        paramMap("log") = URLDecoder.decode(paramMap("log"), "utf-8")
      }
      val allJson = JSON.parseObject(paramMap("log"))
      val time = allJson.get("itime").toString
      appkey = paramMap.getOrElse("appkey", "-")
      if ("-" != appkey){
        for (i <- fields){
          i match {
            case "country" => res += i -> paramMap.getOrElse("country", "-")
            case "province" => res += i -> paramMap.getOrElse("province", "-")
            case "city" => res += i -> paramMap.getOrElse("city", "-")
            case "isp" => res += i -> paramMap.getOrElse("isp", "-")
            case "appkey" => res += i -> paramMap.getOrElse("appkey", "-")
            case "ltype" => res += i -> paramMap.getOrElse("ltype", "-")
            case "value" => res += i -> allJson.get("value").toString
            case "dt" => res += i -> time.split(" ")(0)
            case "hour" =>res += i -> time.split(" ")(1).split(":")(0)
            case "mins" =>res += i -> time.split(" ")(1).split(":")(1)
            case _ => res += i -> get2Json(allJson, i)
          }
        }
      }
      jsonStr = new scala.util.parsing.json.JSONObject(res).toString()
      return jsonStr
    }catch {
      case e: Exception => logger.error("Parse the message failed ..., " + e)
        println(e)
    }
    jsonStr
  }

  /**
    * 存在二级json的问题，一般value中不取出来，如果取出来就会出现空指针，此时去除二级json即可
    * @param allJson log的Json
    * @param field 字段key
    * @return 返回一级或者二级的value
    */
  def get2Json(allJson: com.alibaba.fastjson.JSONObject, field: String): String ={
    var res = "-"
    try{
      res = allJson.get(field).toString
    }catch {
      case e: NullPointerException => res = JSON.parseObject(allJson.get("value").toString).get(field).toString
    }
    return res
  }

  /**
    * 如果㤇解析的话，解密log或者ltype
    * @param logStr 原始日志
    * @return 解密后日志
    */
  def decode(logStr: String): String = {
    val decryptStr = " !_#$%&'()*+,-.ABCDEFGHIJKLMNOP?@/0123456789:;<=>QRSTUVWXYZ[\\]^\"`nopqrstuvwxyzabcdefghijklm{|}~"
    var resLogStr = ""
    val realLog = URLDecoder.decode(logStr, "utf-8")
    for(i <- 0 to realLog.length - 1){
      var ch = realLog.charAt(i)
      if(ch.toInt >= 32 && ch.toInt <= 126){
        resLogStr +=  decryptStr.charAt(ch.toInt - 32)
      }else{
        resLogStr += ch
      }
    }
    resLogStr
  }
}
