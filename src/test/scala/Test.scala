import java.sql.DriverManager

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
    Class.forName("com.mysql.cj.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://150.138.123.221:3306/dt_realtime", "tv", "Tv123!@#")
    val ps = conn.prepareStatement("select * from dt_realtime.tv_active")
    val res = ps.executeQuery()
    while (res.next()){
      println(res.getString(1))
    }
  }
}
