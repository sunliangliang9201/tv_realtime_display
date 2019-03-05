import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig

import scala.util.parsing.json.JSONObject

/**
  * desc
  *
  * @author sunliangliang 2019-03-01 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object Test {

  def main(args: Array[String]): Unit = {
    var m = Map[String, String]("city" -> "聊城市", "country" -> "中国", "province" -> "山东省", "ltype" -> "heart", "isp" -> "联通", "uuid" -> "60000AM4406T17B66054_CCDD")
    m += "a" -> "b"
    val a = new JSONObject(m)
    println(a)
    println(a.getClass)
  }
}
